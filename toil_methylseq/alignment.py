import os
import json
from multiprocessing import Pool
from pathlib import Path
from typing import List

import boto3
from toil.lib.docker import apiDockerCall

from aws_utils import parse_prefix_and_bucket, download_to_location
from domain import PairedEndReadShard, S3OutputLocation, ResourceRequirement


class AlignmentConsts:
    mates_1_raw_fq = "mates_1.fastq"
    mates_2_raw_fq = "mates_2.fastq"
    #  TODO change this name?
    mates_1_trimmed_fq = "mates_1_val_1.fq.gz"
    mates_2_trimmed_fq = "mates_2_val_2.fq.gz"
    bismark_output_bam = "mates_1_val_1_bismark_bt2_pe.bam"
    bismark_output_report = "mates_1_val_1_bismark_bt2_PE_report.txt"
    bismark_deduplicated_bam = "mates_1_val_1_bismark_bt2_pe.deduplicated.bam"
    bismark_deduplication_report = (
        "mates_1_val_1_bismark_bt2_pe.deduplication_report.txt"
    )


def download_file_from_s3(work):
    bucket, key, destination = work
    client = boto3.client("s3")
    client.download_file(bucket, key, destination)


def download_bismark_files(*, tempdir: str, bismark_index: str, bucket: str):
    client = boto3.client("s3")
    ga_conversion_files = []
    ct_conversion_files = []
    next_token = ""

    base_kwargs = {
        "Bucket": bucket,
        "Prefix": bismark_index,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != "":
            kwargs.update({"ContinuationToken": next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get("Contents")
        for i in contents:
            k = i.get("Key")
            assert k is not None
            if "GA_conversion" in k:
                ga_conversion_files.append(k)
            if "CT_conversion" in k:
                ct_conversion_files.append(k)
        next_token = results.get("NextContinuationToken")

    assert len(ga_conversion_files) == len(ct_conversion_files)

    bismark_genome_path = os.path.join(tempdir, "Bisulfite_Genome")
    ga_genome_path = Path(os.path.join(bismark_genome_path, "GA_conversion"))
    ct_genome_path = Path(os.path.join(bismark_genome_path, "CT_conversion"))

    ga_genome_path.mkdir(parents=True, exist_ok=True)
    ct_genome_path.mkdir(parents=True, exist_ok=True)

    ga_file_maker = lambda x: os.path.join(str(ga_genome_path), x.split("/")[-1])
    ct_file_maker = lambda x: os.path.join(str(ct_genome_path), x.split("/")[-1])

    work = []
    for ga_key, ct_key in zip(ga_conversion_files, ct_conversion_files):
        ga_file = ga_file_maker(ga_key)
        ct_file = ct_file_maker(ct_key)
        work.append((bucket, ga_key, ga_file))
        work.append((bucket, ct_key, ct_file))
        print(ga_file, ct_file, bucket)

    with Pool(processes=4) as pool:
        pool.map(download_file_from_s3, work)


class BismarkShardAligner:
    def __init__(
        self,
        job,
        *,
        shard: PairedEndReadShard,
        apps_image: str,
        utils_image: str,
        bismark_index_url: str,
        bismark_genome_uri: str,
        shard_idx: int,
        s3_location: S3OutputLocation,
    ):
        self.job = job
        self.apps_image = apps_image
        self.utils_image = utils_image
        self.bismark_index_url = bismark_index_url
        self.bismark_genome_uri = bismark_genome_uri
        self.shard = shard
        self.shard_idx = shard_idx
        self.s3_output = s3_location
        self.tempdir = job.fileStore.getLocalTempDir()

        # these are filled in as the processing progresses
        self.mates_1_trimmed_path = None
        self.mates_2_trimmed_path = None
        self.bismark_alignment_path = None
        self.bismark_deduplicated_bam_path = None

    def _run_trim_galore(self):
        _output = apiDockerCall(
            self.job,
            user="root",
            image=self.apps_image,
            volumes={self.tempdir: {"bind": "/io", "mode": "rw"}},
            parameters=[
                "trim_galore",
                "--fastqc",
                "--gzip",
                "--paired",
                f"/io/{AlignmentConsts.mates_1_raw_fq}",
                f"/io/{AlignmentConsts.mates_2_raw_fq}",
                "-o",
                "/io/",
            ],
        )
        trimmed_1 = os.path.join(self.tempdir, AlignmentConsts.mates_1_trimmed_fq)
        trimmed_2 = os.path.join(self.tempdir, AlignmentConsts.mates_2_trimmed_fq)

        assert os.path.exists(
            trimmed_1
        ), f"trimmed 1 missing, {os.listdir(self.tempdir)}"
        assert os.path.exists(
            trimmed_2
        ), f"trimmed 2 missing, {os.listdir(self.tempdir)}"
        self.mates_1_trimmed_path = trimmed_1
        self.mates_2_trimmed_path = trimmed_2

    def _run_bismark_alignment(self):
        assert self.mates_1_trimmed_path is not None
        assert self.mates_2_trimmed_path is not None

        bucket, prefix = parse_prefix_and_bucket(self.bismark_index_url)
        genome_dir = os.path.join(self.tempdir, "genome")
        download_bismark_files(tempdir=genome_dir, bismark_index=prefix, bucket=bucket)
        download_to_location(s3_url=self.bismark_genome_uri, temp_dir=genome_dir)

        _ouput = apiDockerCall(
            self.job,
            user="root",
            image=self.apps_image,
            volumes={self.tempdir: {"bind": "/io", "mode": "rw"}},
            parameters=[
                "bismark",
                "-1",
                f"/io/{AlignmentConsts.mates_1_trimmed_fq}",
                "-2",
                f"/io/{AlignmentConsts.mates_2_trimmed_fq}",
                "--genome",
                "/io/genome/",
                "-o",
                "/io/",
            ],
        )

        output_alignment_path = os.path.join(
            self.tempdir, AlignmentConsts.bismark_output_bam
        )
        output_alignment_report = os.path.join(
            self.tempdir, AlignmentConsts.bismark_output_report
        )

        assert os.path.exists(
            output_alignment_path
        ), f"missing alignment {os.listdir(self.tempdir)}"
        assert os.path.exists(
            output_alignment_report
        ), f"missing alignment {os.listdir(self.tempdir)}"

        alignment_file_id = self.job.fileStore.writeGlobalFile(output_alignment_path)
        report_file_id = self.job.fileStore.writeGlobalFile(output_alignment_report)

        self.job.fileStore.exportFile(
            alignment_file_id,
            self.s3_output.to_url(
                f"{self.shard_idx}_{AlignmentConsts.bismark_output_bam}"
            ),
        )
        self.job.fileStore.exportFile(
            report_file_id,
            self.s3_output.to_url(
                f"{self.shard_idx}_{AlignmentConsts.bismark_output_report}"
            ),
        )
        self.bismark_alignment_path = output_alignment_path

    def _run_bismark_deduplicate(self):
        assert self.bismark_alignment_path is not None

        _ouput = apiDockerCall(
            self.job,
            user="root",
            image=self.apps_image,
            volumes={self.tempdir: {"bind": "/io", "mode": "rw"}},
            parameters=[
                "deduplicate_bismark",
                "-p",
                f"/io/{AlignmentConsts.bismark_output_bam}",
                "--output_dir",
                "/io/",
            ],
        )
        deduplicated_bam_path = os.path.join(
            self.tempdir, AlignmentConsts.bismark_deduplicated_bam
        )
        deduplication_report_path = os.path.join(
            self.tempdir, AlignmentConsts.bismark_deduplication_report
        )

        assert os.path.exists(
            deduplicated_bam_path
        ), f"missing deduped alignment {os.listdir(self.tempdir)}"
        assert os.path.exists(
            deduplication_report_path
        ), f"missing deduped alignment report {os.listdir(self.tempdir)}"

        deduplication_report_file_id = self.job.fileStore.writeGlobalFile(
            deduplication_report_path
        )
        self.job.fileStore.exportFile(
            deduplication_report_file_id,
            self.s3_output.to_url(
                f"{self.shard_idx}_{AlignmentConsts.bismark_deduplication_report}"
            ),
        )
        self.bismark_deduplicated_bam_path = deduplicated_bam_path

    def _shard_alignment_by_chrom(self) -> dict:
        assert self.bismark_deduplicated_bam_path is not None
        chrom_files = apiDockerCall(
            self.job,
            user="root",
            image=self.utils_image,
            volumes={self.tempdir: {"bind": "/io", "mode": "rw"}},
            parameters=[
                "bam-sort",
                "-i",
                f"/io/{AlignmentConsts.bismark_deduplicated_bam}",
            ],
        )
        chrom_files = json.loads(chrom_files)

        results = dict()
        for chrom, alignment_file in chrom_files:
            alignment_file_path = os.path.join(self.tempdir, alignment_file)
            chrom_file_id = self.job.fileStore.writeGlobalFile(alignment_file_path)
            # self.job.fileStore.exportFile(
            #     chrom_file_id,
            #     self.s3_output.to_url(
            #         f"{self.shard_idx}_{chrom}_{AlignmentConsts.bismark_deduplicated_bam}"
            #     ),
            # )
            assert chrom not in results, f"repeat of {chrom}?, output {chrom_files}"
            results[chrom] = chrom_file_id
        return results

    def run_alignment_on_shard(self):
        self.job.fileStore.readGlobalFile(
            fileStoreID=self.shard.mate1_fid,
            userPath=os.path.join(self.tempdir, AlignmentConsts.mates_1_raw_fq),
        )
        self.job.fileStore.readGlobalFile(
            fileStoreID=self.shard.mate2_fid,
            userPath=os.path.join(self.tempdir, AlignmentConsts.mates_2_raw_fq),
        )
        assert os.path.exists(
            os.path.join(self.tempdir, AlignmentConsts.mates_1_raw_fq)
        )
        assert os.path.exists(
            os.path.join(self.tempdir, AlignmentConsts.mates_2_raw_fq)
        )
        self._run_trim_galore()
        self._run_bismark_alignment()
        self._run_bismark_deduplicate()
        chrom_file_ids = self._shard_alignment_by_chrom()
        return chrom_file_ids


def align_shard(
    job,
    shard: PairedEndReadShard,
    shard_idx: int,
    *,
    apps_image: str,
    utils_image: str,
    bismark_index_url: str,
    bismark_genome_uri: str,
    s3_location: S3OutputLocation,
):
    shard_aligner = BismarkShardAligner(
        job,
        shard=shard,
        apps_image=apps_image,
        utils_image=utils_image,
        bismark_index_url=bismark_index_url,
        bismark_genome_uri=bismark_genome_uri,
        shard_idx=shard_idx,
        s3_location=s3_location,
    )
    result = shard_aligner.run_alignment_on_shard()

    return result


def dummy_align_shard(
    job,
    shard: PairedEndReadShard,
    shard_idx: int,
    *,
    apps_image: str,
    utils_image: str,
    bismark_index_url: str,
    bismark_genome_uri: str,
    s3_location: S3OutputLocation,
):
    import time

    print("DUMMY")
    time.sleep(3)
    return {"chr1": f"{shard_idx}-foo", "chr2": f"{shard_idx}-bar"}


def alignment_root_job(
    job,
    shards: List[PairedEndReadShard],
    apps_image: str,
    utils_image: str,
    bismark_index_url: str,
    bismark_genome_uri: str,
    s3_location: S3OutputLocation,
):
    results = []
    for i, shard in enumerate(shards):
        memory = ResourceRequirement.convert_size(int(shard.mate1_fid.size * 1.25))
        disc = ResourceRequirement.convert_size(int(shard.mate1_fid.size * 1.1))

        raise NotImplementedError("add disc and memory reqs for reference")

        shard_alignment = job.addChildJobFn(
            align_shard,
            cores=4,
            # dummy_align_shard,
            # cores=0.1,
            memory=memory.to_string(),
            disk=disc.to_string(),
            name=f"alignment-{i}",
            shard=shard,
            shard_idx=i,
            apps_image=apps_image,
            utils_image=utils_image,
            bismark_index_url=bismark_index_url,
            bismark_genome_uri=bismark_genome_uri,
            s3_location=s3_location,
        )
        results.append(shard_alignment)

    return [r.rv() for r in results]

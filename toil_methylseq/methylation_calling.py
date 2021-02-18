import os
from collections import defaultdict
from typing import List

from toil.fileStores import FileID
from toil.lib.docker import apiDockerCall

from domain import S3OutputLocation


def run_methylation_extractor(
    job, *, chrom: str, apps_image: str, temp_dir: str, s3_output: S3OutputLocation
) -> dict:
    chrom_bam = f"{chrom}.bam"
    _samtools_cat_output = apiDockerCall(
        job,
        user="root",
        image=apps_image,
        volumes={temp_dir: {"bind": "/io", "mode": "rw"}},
        parameters=[
            "samtools",
            "cat",
            "/io/*.bam",
            "-o" f"/io/{chrom_bam}",
        ],
    )
    merged_bam_path = os.path.join(temp_dir, chrom_bam)
    assert os.path.exists(merged_bam_path), f"missing merged bam {os.listdir(temp_dir)}"

    chrom_bam_file_id = job.fileStore.writeGlobalFile(merged_bam_path)
    job.fileStore.exportFile(
        chrom_bam_file_id, s3_output.to_url(f"{chrom}_methylation_input.bam")
    )

    _bismark_methylation_calling_output = apiDockerCall(
        job,
        user="root",
        image=apps_image,
        volumes={temp_dir: {"bind": "/io", "mode": "rw"}},
        parameters=[
            "bismark_methylation_extractor",
            "--ignore_r2",
            "2",
            "--ignore_3prime_r2",
            "2",
            "--bedGraph",
            "--gzip",
            "-p",
            "--counts",
            "--no_overlap",
            "--report",
            "--o",
            "/io",
            "--report",
            f"/io/{chrom_bam}",
        ],
    )

    bed_graph_filename = f"{chrom}.bedGraph.gz"
    bismark_cov_filename = f"{chrom}.bismark.cov.gz"
    bed_graph_path = os.path.join(temp_dir, bed_graph_filename)
    bismark_cov_path = os.path.join(temp_dir, bismark_cov_filename)
    assert os.path.exists(bed_graph_path), f"missing bedGraph {os.listdir(temp_dir)}"
    assert os.path.exists(
        bismark_cov_path
    ), f"missing bismark cov {os.listdir(temp_dir)}"
    bed_graph_file_id = job.fileStore.writeGlobalFile(bed_graph_path)
    bismark_cov_file_id = job.fileStore.writeGlobalFile(bismark_cov_path)

    return {
        bed_graph_filename: bed_graph_file_id,
        bismark_cov_filename: bismark_cov_file_id,
    }


def call_methylation(
    job,
    chrom: str,
    file_ids: List[FileID],
    apps_image: str,
    s3_output: S3OutputLocation,
):
    temp_dir = job.fileStore.getLocalTempDir()
    for i, file_id in enumerate(file_ids):
        file_id_path = os.path.join(temp_dir, f"{i}_{chrom}.bam")
        job.fileStore.readGlobalFile(file_id, file_id_path)
        assert os.path.exists(file_id_path)

    bismark_reslts = run_methylation_extractor(
        job, apps_image=apps_image, chrom=chrom, temp_dir=temp_dir, s3_output=s3_output
    )
    for filename, file_id in bismark_reslts.items():
        job.fileStore.exportFile(file_id, s3_output.to_url(filename))
    return "ok"


def methylation_calling_root_job(
    job, *, apps_image: str, chrom_file_ids: List[dict], s3_output: S3OutputLocation
):
    chrom_to_file_ids = defaultdict(list)
    for mapping in chrom_file_ids:
        for chrom, file_id in mapping.items():
            chrom_to_file_ids[chrom].append(file_id)

    results = []
    for chrom, file_ids in chrom_to_file_ids.items():
        results.append(
            job.addChildJobFn(
                call_methylation,
                name=f"{chrom}_methylation_calling",
                chrom=chrom,
                file_ids=file_ids,
                apps_image=apps_image,
                s3_output=s3_output,
                disk="40G",
                memory="40G",
                cores=4,
            )
        )

    return [r.rv() for r in results]

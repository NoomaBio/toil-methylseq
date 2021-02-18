import json
from argparse import ArgumentParser
from pathlib import Path
from typing import List

from toil.common import Toil
from toil.job import Job

from domain import PairedEndReads, S3OutputLocation, ToilMethylseqConfig
from fastqc import run_fastqc_root
from preprocessing import shard_input_fastq
from methylation_calling import methylation_calling_root_job
from alignment import alignment_root_job


def parse_config(path: str) -> ToilMethylseqConfig:
    assert Path(path).exists(), f"missing config at {path}"
    config = dict()

    # config[
    #     "bismark_index_url"
    # ] = "s3://rand-dev/toil-methylseq/genome-fake/Bisulfite_Genome/"
    # config[
    #     "bismark_genome_uri"
    # ] = "s3://rand-dev/toil-methylseq/genome-fake/fake-genome.fa"

    with open(path, "r") as fh:
        raw_config = json.load(fh)
        try:
            config["paired_reads"] = [
                PairedEndReads.parse(raw) for raw in raw_config["paired_reads"]
            ]
            config["apps_image"] = raw_config["apps_image"]
            config["utils_image"] = raw_config["utils_image"]
            config["s3_output"] = S3OutputLocation.parse(raw_config["s3_output"])
            config["bismark_genome_uri"] = raw_config["bismark_reference_genome_fasta"]
            config["bismark_index_url"] = raw_config["bismark_genome_index"]
            config["bins"] = raw_config.get("bins", 4)
        except KeyError as e:
            raise KeyError(f"config missing field {e}")

        return ToilMethylseqConfig(**config)


def root_alignment_job(job, config: ToilMethylseqConfig):
    # read sharding
    fastq_shards = job.addChildJobFn(
        shard_input_fastq,
        reads=config.paired_reads,
        utils_image=config.utils_image,
        bins=config.bins,
    ).rv()

    alignments: List[dict] = job.addFollowOnJobFn(
        alignment_root_job,
        shards=fastq_shards,
        apps_image=config.apps_image,
        utils_image=config.utils_image,
        bismark_index_url=config.bismark_index_url,
        bismark_genome_uri=config.bismark_genome_uri,
        s3_location=config.s3_output,
    ).rv()

    return alignments


def run_methylation_calling(job, alignments: List[dict], config: ToilMethylseqConfig):
    job.addChildJobFn(
        methylation_calling_root_job,
        apps_image=config.apps_image,
        chrom_file_ids=alignments,
        s3_output=config.s3_output,
    )
    return "OK"


def run_methylseq(job: Job, config: ToilMethylseqConfig):
    # _fastqc_job = job.addChildJobFn(
    #     run_fastqc_root,
    #     reads=config.paired_reads,
    #     s3_output=config.s3_output,
    #     apps_image=config.apps_image,
    #     name="fastqc_root_job",
    # ).rv()

    alignments = job.addChildJobFn(root_alignment_job, config=config).rv()

    methylation_calling = job.addFollowOnJobFn(
        run_methylation_calling, alignments=alignments, config=config
    ).rv()

    return methylation_calling


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--config", required=True, action="store", help="run configuration"
    )
    Job.Runner.addToilOptions(parser)
    options = parser.parse_args()

    toil_config = parse_config(options.config)
    with Toil(options) as workflow:
        if not workflow.options.restart:
            root_job = Job.wrapJobFn(run_methylseq, config=toil_config)
            result = workflow.start(root_job)
            print(result)
        else:
            workflow.restart()


if __name__ == "__main__":
    main()

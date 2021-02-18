import json
import itertools
import os
import boto3
from typing import List

from toil.fileStores import FileID
from toil.lib.docker import apiDockerCall

from aws_utils import download_to_location, estimate_resource_requirements
from domain import PairedEndReads, PairedEndReadShard, ArtifactResourceRequirements


def _run_sharding(job, *, uri: str, utils_image: str, bins: int) -> List[FileID]:
    temp_dir = job.fileStore.getLocalTempDir()
    _, reads_filename = download_to_location(s3_url=uri, temp_dir=temp_dir)
    sharding_output = apiDockerCall(
        job,
        user="root",
        image=utils_image,
        volumes={temp_dir: {"bind": "/io", "mode": "rw"}},
        parameters=["fastq-split", "-i", f"/io/{reads_filename}", "-b", str(bins)],
    )
    sharding_output = json.loads(sharding_output)
    output_files = list(sharding_output.values())
    assert len(output_files) == 1
    output_files = output_files[0]
    file_ids = [
        job.fileStore.writeGlobalFile(os.path.join(temp_dir, shard))
        for shard in output_files
    ]
    return file_ids


def coalesce_shards(
    mate1_shards: List[FileID], mate2_shards: List[FileID], reads_name: str
) -> List[PairedEndReadShard]:
    assert len(mate1_shards) == len(mate2_shards)
    return [
        PairedEndReadShard(mate1_fid=x, mate2_fid=y, name=reads_name)
        for x, y in zip(mate1_shards, mate2_shards)
    ]


def shard_reads(
    job, paired_end_reads: PairedEndReads, utils_image: str, bins: int
) -> List[PairedEndReadShard]:
    mates_1_shards = job.addChildJobFn(
        _run_sharding,
        uri=paired_end_reads.uri_1,
        utils_image=utils_image,
        name=f"sharding_{paired_end_reads.uri_1}",
        bins=bins,
    )
    mates_2_shards = job.addChildJobFn(
        _run_sharding,
        uri=paired_end_reads.uri_2,
        utils_image=utils_image,
        name=f"sharding_{paired_end_reads.uri_2}",
        bins=bins,
    )

    paired_end_shards = job.addFollowOnFn(
        coalesce_shards,
        mate1_shards=mates_1_shards.rv(),
        mate2_shards=mates_2_shards.rv(),
        reads_name=paired_end_reads.name,
    ).rv()

    return paired_end_shards


def flatten_paired_end_shards(
    shards: List[List[PairedEndReadShard]],
) -> List[PairedEndReadShard]:
    return list(itertools.chain(*shards))


def get_resource_requirements_for_reads(
    reads: PairedEndReads,
) -> ArtifactResourceRequirements:
    uri1_res = estimate_resource_requirements(reads.uri_1, reads.storage)
    uri2_res = estimate_resource_requirements(reads.uri_2, reads.storage)
    return uri1_res + uri2_res


def shard_input_fastq(job, reads: List[PairedEndReads], utils_image: str, bins: int):
    resource_requirements = [
        get_resource_requirements_for_reads(paired_end_reads) * 2
        for paired_end_reads in reads
    ]
    shards = [
        job.addChildJobFn(
            shard_reads,
            paired_end_reads=pe,
            utils_image=utils_image,
            bins=bins,
            disk=resource_requirements.disc.to_string(),
            memory=resource_requirements.disc.to_string(),
            name=f"sharding_{pe.name}",
        )
        for (resource_requirements, pe) in zip(resource_requirements, reads)
    ]
    shards = [r.rv() for r in shards]
    return job.addFollowOnFn(flatten_paired_end_shards, shards=shards).rv()

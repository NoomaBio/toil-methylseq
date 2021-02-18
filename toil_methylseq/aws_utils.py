import math
import os
from typing import List

import boto3

from domain import Storage, ArtifactResourceRequirements, ResourceRequirement


def _split_s3_url(s3_url) -> (str, List[str]):
    _, rest = s3_url.split("s3://")
    bucket, *rest = rest.split("/")
    return bucket, rest


def parse_prefix_and_bucket(s3_url) -> (str, str):
    try:
        bucket, rest = _split_s3_url(s3_url)
        return bucket, "/".join(rest)
    except ValueError as e:
        raise ValueError("illegal s3 url") from e


def parse_s3_url_key_bucket_filename(s3_url) -> (str, str, str):
    try:
        bucket, rest = _split_s3_url(s3_url)
        filename = rest[-1]
        return bucket, "/".join(rest), filename

    except ValueError as e:
        raise ValueError("illegal s3 url") from e


def download_to_location(*, s3_url: str, temp_dir: str) -> (str, str):
    s3 = boto3.client("s3")
    bucket, key, filename = parse_s3_url_key_bucket_filename(s3_url)
    temp_filepath = os.path.join(temp_dir, filename)
    s3.download_file(bucket, key, temp_filepath)
    return temp_filepath, filename


def estimate_resource_requirements(
    uri: str,
    storage: Storage,
    mem_expand_buffer: float = 1.25,
    disc_expand_buffer: float = 1.1,
) -> ArtifactResourceRequirements:
    if storage.value == Storage.S3.value:
        s3 = boto3.client("s3")
        bucket, key, _ = parse_s3_url_key_bucket_filename(uri)
        content_length = s3.get_object(Bucket=bucket, Key=key)["ContentLength"]
        disc = ResourceRequirement.convert_size(
            int(disc_expand_buffer * content_length)
        )
        memory = ResourceRequirement.convert_size(
            int(mem_expand_buffer * content_length)
        )
        return ArtifactResourceRequirements(memory=memory, disc=disc)
    else:
        raise NotImplementedError("local file estimate?")

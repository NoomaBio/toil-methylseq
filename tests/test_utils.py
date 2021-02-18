from domain import PairedEndReads, ArtifactResourceRequirements, ResourceRequirement
from preprocessing import get_resource_requirements_for_reads
from aws_utils import parse_s3_url_key_bucket_filename, estimate_resource_requirements


def test_s3_key_parse():
    bucket, key, filename = parse_s3_url_key_bucket_filename(
        "s3://bucket/the/key/to/happyness.json"
    )
    assert bucket == "bucket"
    assert key == "the/key/to/happyness.json"
    assert filename == "happyness.json"


def test_parse_config():
    test_reads = {
        "testdata": {
            "1": "s3://rand-dev/toil-methylseq/testdata/reads_1.fastq",
            "2": "s3://rand-dev/toil-methylseq/testdata/reads_2.fastq",
        }
    }

    paired_end_reads = PairedEndReads.parse(test_reads)
    assert paired_end_reads.name == "testdata"
    assert (
        paired_end_reads.uri_1 == "s3://rand-dev/toil-methylseq/testdata/reads_1.fastq"
    )
    assert (
        paired_end_reads.uri_2 == "s3://rand-dev/toil-methylseq/testdata/reads_2.fastq"
    )
    assert paired_end_reads.storage.value == 1

    test_reads = {
        "testdata": {
            "1": "file://rand-dev/toil-methylseq/testdata/reads_1.fastq",
            "2": "file://rand-dev/toil-methylseq/testdata/reads_2.fastq",
        }
    }
    paired_end_reads = PairedEndReads.parse(test_reads)
    assert paired_end_reads.name == "testdata"
    assert (
        paired_end_reads.uri_1
        == "file://rand-dev/toil-methylseq/testdata/reads_1.fastq"
    )
    assert (
        paired_end_reads.uri_2
        == "file://rand-dev/toil-methylseq/testdata/reads_2.fastq"
    )
    assert paired_end_reads.storage.value == 2


def test_add_artifact_resource_requirements():
    reqs1 = ArtifactResourceRequirements(
        memory=ResourceRequirement(amount=10, unit="B"),
        disc=ResourceRequirement(amount=10, unit="MB"),
    )
    reqs2 = ArtifactResourceRequirements(
        memory=ResourceRequirement(amount=10, unit="B"),
        disc=ResourceRequirement(amount=10, unit="MB"),
    )

    expected = ArtifactResourceRequirements(
        memory=ResourceRequirement(amount=20, unit="B"),
        disc=ResourceRequirement(amount=20, unit="MB"),
    )

    sum_ = reqs1 + reqs2
    assert sum_ == expected

    double_recs1 = reqs1 * 2
    assert double_recs1 == expected


def test_resource_requirement_convert():
    a = ResourceRequirement(amount=1, unit="GB")
    b = a.convert_to("MB")
    assert b == ResourceRequirement(amount=1000, unit="MB")
    a = ResourceRequirement(amount=10, unit="MB")
    b = a.convert_to("GB")
    assert b == ResourceRequirement(amount=0.01, unit="GB")


def test_estimate_read_size():
    test_reads = {
        "testdata": {
            "1": "s3://rand-dev/toil-methylseq/testdata/reads_1.fastq",
            "2": "s3://rand-dev/toil-methylseq/testdata/reads_2.fastq",
        }
    }
    paired_end_reads = PairedEndReads.parse(test_reads)
    resource_reqs = get_resource_requirements_for_reads(paired_end_reads)

    expected = ArtifactResourceRequirements(
        memory=ResourceRequirement(amount=52, unit="MB"),
        disc=ResourceRequirement(amount=46, unit="MB"),
    )

    assert resource_reqs == expected

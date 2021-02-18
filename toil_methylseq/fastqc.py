import itertools
import os
from typing import List, Tuple

from toil.fileStores import FileID
from toil.lib.docker import apiDockerCall

from domain import PairedEndReads, S3OutputLocation
from aws_utils import download_to_location


def _run_on_reads(job, s3_url: str, apps_image: str) -> List[Tuple[str, FileID]]:
    temp_dir = job.fileStore.getLocalTempDir()
    reads_file_path, reads_filename = download_to_location(
        s3_url=s3_url, temp_dir=temp_dir
    )
    stdout = apiDockerCall(
        job,
        user="root",
        image=apps_image,
        volumes={temp_dir: {"bind": "/io", "mode": "rw"}},
        parameters=["fastqc", "--quiet", "--threads", "2", f"/io/{reads_filename}"],
    )
    job.log(f"{stdout.decode('utf-8')}")

    result_file_ids = []
    for suffix in ("_fastqc.html", "_fastqc.zip"):
        reads_filename_root = reads_filename.split(".")[0]
        result_filename = f"{reads_filename_root}{suffix}"
        result_file_path = os.path.join(temp_dir, result_filename)
        assert os.path.exists(
            result_file_path
        ), f"missing fastqc file {result_filename}, {os.listdir(temp_dir)}"
        file_id = job.fileStore.writeGlobalFile()  # this should be writing a file...?
        result_file_ids.append((result_filename, file_id))
    return result_file_ids


def run_fastqc_on_files(
    job, reads: List[PairedEndReads], apps_image: str
) -> List[List[tuple]]:
    s3_uris = itertools.chain(*[[r.uri_1, r.uri_2] for r in reads])
    results = [
        job.addChildJobFn(
            _run_on_reads,
            s3_url=uri,
            apps_image=apps_image,
            name=f"fastqc_on_{uri}",
            disk="20G",
            memory="10G",
        )
        for uri in s3_uris
    ]

    return [r.rv() for r in results]


def publish_fastqc_results(
    job,
    html_filename: str,
    html_file_id: FileID,
    zip_filename: str,
    zip_file_id: FileID,
    s3_location: S3OutputLocation,
):

    job.fileStore.exportFile(html_file_id, s3_location.to_url(html_filename))
    job.fileStore.exportFile(zip_file_id, s3_location.to_url(zip_filename))


def publish_results_job(job, results: list, s3_output: S3OutputLocation):
    for read_results in results:
        assert len(read_results) == 2
        html_filename, html_fileid = read_results[0]
        zip_filename, zip_fileid = read_results[1]
        job.addChildJobFn(
            publish_fastqc_results,
            html_filename=html_filename,
            html_file_id=html_fileid,
            zip_filename=zip_filename,
            zip_file_id=zip_fileid,
            s3_location=s3_output,
        )


def run_fastqc_root(
    job, reads: List[PairedEndReads], *, s3_output: S3OutputLocation, apps_image: str
):
    run_fastqc_job = job.addChildJobFn(
        run_fastqc_on_files,
        reads=reads,
        apps_image=apps_image,
        name="run_fastqc_on_files",
    )
    job.addFollowOnJobFn(
        publish_results_job,
        results=run_fastqc_job.rv(),
        s3_output=s3_output,
        name="publish_fastqc_results",
    )

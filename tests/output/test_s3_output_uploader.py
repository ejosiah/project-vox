# tests/output/test_s3_output_uploader.py

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from core.output.s3_output_uploader import S3OutputUploader


def test_s3_output_uploader_uploads_all_outputs_with_job_id_prefix(tmp_path):
    txt_file = tmp_path / "transcript.txt"
    json_file = tmp_path / "transcript.json"
    txt_file.write_text("hello", encoding="utf-8")
    json_file.write_text("{}", encoding="utf-8")

    s3_client = Mock()

    uploader = S3OutputUploader(
        bucket_name="vox-output",
        prefix="jobs",
        s3_client=s3_client,
    )

    result = uploader.upload(
        job_id="job-123",
        outputs={
            "txt": str(txt_file),
            "json": str(json_file),
        },
    )

    assert result.job_id == "job-123"
    assert set(result.outputs.keys()) == {"txt", "json"}

    s3_client.upload_file.assert_any_call(
        str(txt_file),
        "vox-output",
        "jobs/job-123/transcript.txt",
    )
    s3_client.upload_file.assert_any_call(
        str(json_file),
        "vox-output",
        "jobs/job-123/transcript.json",
    )

    assert result.outputs["txt"].destination == "s3://vox-output/jobs/job-123/transcript.txt"
    assert result.outputs["json"].destination == "s3://vox-output/jobs/job-123/transcript.json"


def test_s3_output_uploader_raises_when_output_file_missing(tmp_path):
    missing_file = tmp_path / "missing.txt"

    uploader = S3OutputUploader(
        bucket_name="vox-output",
        s3_client=Mock(),
    )

    with pytest.raises(FileNotFoundError, match="Generated output does not exist"):
        uploader.upload(
            job_id="job-123",
            outputs={"txt": str(missing_file)},
        )


def test_s3_output_uploader_supports_empty_prefix(tmp_path):
    txt_file = tmp_path / "transcript.txt"
    txt_file.write_text("hello", encoding="utf-8")

    s3_client = Mock()

    uploader = S3OutputUploader(
        bucket_name="vox-output",
        prefix="",
        s3_client=s3_client,
    )

    result = uploader.upload(
        job_id="job-123",
        outputs={"txt": str(txt_file)},
    )

    s3_client.upload_file.assert_called_once_with(
        str(txt_file),
        "vox-output",
        "job-123/transcript.txt",
    )
    assert result.outputs["txt"].destination == "s3://vox-output/job-123/transcript.txt"
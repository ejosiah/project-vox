# tests/pipeline/stages/test_upload_output_stage.py

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from core.output.uploader import UploadResult, UploadedOutput
from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages.upload_output_stage import UploadOutputStage


def test_upload_output_stage_uploads_generated_outputs_and_updates_context_metadata(tmp_path):
    txt_file = tmp_path / "transcript.txt"
    json_file = tmp_path / "transcript.json"
    txt_file.write_text("hello", encoding="utf-8")
    json_file.write_text("{}", encoding="utf-8")

    generated_outputs = {
        "txt": str(txt_file),
        "json": str(json_file),
    }

    upload_result = UploadResult(
        job_id="job-123",
        outputs={
            "txt": UploadedOutput(
                output_type="txt",
                source_path=txt_file,
                destination="s3://bucket/jobs/job-123/transcript.txt",
            ),
            "json": UploadedOutput(
                output_type="json",
                source_path=json_file,
                destination="s3://bucket/jobs/job-123/transcript.json",
            ),
        },
    )

    uploader = Mock()
    uploader.upload.return_value = upload_result

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"generated_outputs": generated_outputs},
    )

    stage = UploadOutputStage(uploader=uploader)

    result = stage.run(context)

    assert result.status == StageStatus.SUCCESS
    assert result.context is context
    assert context.metadata["uploaded_outputs"] is upload_result
    assert result.metadata == {"uploaded_outputs": upload_result}
    uploader.upload.assert_called_once_with(
        job_id="job-123",
        outputs=generated_outputs,
    )


def test_upload_output_stage_raises_when_generated_outputs_missing():
    uploader = Mock()

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={},
    )

    stage = UploadOutputStage(uploader=uploader)

    with pytest.raises(
        ValueError,
        match=r"context\.metadata\['generated_outputs'\] is required for UploadOutputStage",
    ):
        stage.run(context)

    uploader.upload.assert_not_called()


def test_upload_output_stage_skips_when_uploaded_outputs_already_exist(tmp_path):
    txt_file = tmp_path / "transcript.txt"
    txt_file.write_text("hello", encoding="utf-8")

    existing_upload_result = UploadResult(
        job_id="job-123",
        outputs={
            "txt": UploadedOutput(
                output_type="txt",
                source_path=txt_file,
                destination="https://downloads.example.com/job-123/transcript.txt",
                download_url="https://downloads.example.com/job-123/transcript.txt",
            )
        },
    )

    uploader = Mock()

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={
            "generated_outputs": {"txt": str(txt_file)},
            "uploaded_outputs": existing_upload_result,
        },
    )

    stage = UploadOutputStage(uploader=uploader)

    result = stage.run(context)

    assert result.status == StageStatus.SKIPPED
    assert result.context is context
    assert context.metadata["uploaded_outputs"] is existing_upload_result
    assert result.metadata == {"uploaded_outputs": existing_upload_result}
    uploader.upload.assert_not_called()


def test_upload_output_stage_passes_job_id_and_generated_outputs_to_uploader(tmp_path):
    txt_file = tmp_path / "transcript.txt"
    txt_file.write_text("hello", encoding="utf-8")

    generated_outputs = {"txt": str(txt_file)}

    upload_result = UploadResult(job_id="job-456", outputs={})

    uploader = Mock()
    uploader.upload.return_value = upload_result

    context = JobContext(
        job_id="job-456",
        request={"input": "file.mp4"},
        metadata={"generated_outputs": generated_outputs},
    )

    stage = UploadOutputStage(uploader=uploader)
    stage.run(context)

    uploader.upload.assert_called_once_with(
        job_id="job-456",
        outputs=generated_outputs,
    )
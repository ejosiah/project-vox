# tests/pipeline/stages/test_finalize_job_stage.py

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from core.output.uploader import UploadResult, UploadedOutput
from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages.finalize_job_stage import FinalizeJobStage


def test_finalize_job_stage_sets_success_outcome_from_uploaded_outputs(tmp_path):
    txt_file = tmp_path / "transcript.txt"
    json_file = tmp_path / "transcript.json"
    txt_file.write_text("hello", encoding="utf-8")
    json_file.write_text("{}", encoding="utf-8")

    uploaded_outputs = UploadResult(
        job_id="job-123",
        outputs={
            "txt": UploadedOutput(
                output_type="txt",
                source_path=txt_file,
                destination="s3://bucket/job-123/transcript.txt",
            ),
            "json": UploadedOutput(
                output_type="json",
                source_path=json_file,
                destination="s3://bucket/job-123/transcript.json",
            ),
        },
    )

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"uploaded_outputs": uploaded_outputs},
    )

    stage = FinalizeJobStage()
    result = stage.run(context)

    outcome = context.metadata["job_outcome"]

    assert result.status == StageStatus.SUCCESS
    assert result.context is context
    assert result.metadata == {"job_outcome": outcome}

    assert outcome["job_id"] == "job-123"
    assert outcome["status"] == "success"
    assert outcome["output_count"] == 2
    assert outcome["has_uploaded_outputs"] is True
    assert outcome["has_generated_outputs"] is False
    assert datetime.fromisoformat(outcome["finalized_at"])


def test_finalize_job_stage_falls_back_to_generated_outputs_when_uploaded_missing(tmp_path):
    txt_file = tmp_path / "transcript.txt"
    srt_file = tmp_path / "transcript.srt"
    txt_file.write_text("hello", encoding="utf-8")
    srt_file.write_text("1\n...", encoding="utf-8")

    generated_outputs = {
        "txt": str(txt_file),
        "srt": str(srt_file),
    }

    context = JobContext(
        job_id="job-456",
        request={"input": "file.mp4"},
        metadata={"generated_outputs": generated_outputs},
    )

    stage = FinalizeJobStage()
    result = stage.run(context)

    outcome = result.metadata["job_outcome"]

    assert result.status == StageStatus.SUCCESS
    assert outcome["job_id"] == "job-456"
    assert outcome["status"] == "success"
    assert outcome["output_count"] == 2
    assert outcome["has_uploaded_outputs"] is False
    assert outcome["has_generated_outputs"] is True
    assert datetime.fromisoformat(outcome["finalized_at"])


def test_finalize_job_stage_handles_no_outputs():
    context = JobContext(
        job_id="job-789",
        request={"input": "file.mp4"},
        metadata={},
    )

    stage = FinalizeJobStage()
    result = stage.run(context)

    outcome = result.metadata["job_outcome"]

    assert result.status == StageStatus.SUCCESS
    assert outcome["job_id"] == "job-789"
    assert outcome["status"] == "success"
    assert outcome["output_count"] == 0
    assert outcome["has_uploaded_outputs"] is False
    assert outcome["has_generated_outputs"] is False
    assert datetime.fromisoformat(outcome["finalized_at"])


def test_finalize_job_stage_skips_when_job_outcome_already_exists():
    existing_outcome = {
        "job_id": "job-123",
        "status": "success",
        "finalized_at": "2026-03-17T10:00:00+00:00",
        "output_count": 1,
        "has_uploaded_outputs": True,
        "has_generated_outputs": True,
    }

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"job_outcome": existing_outcome},
    )

    stage = FinalizeJobStage()
    result = stage.run(context)

    assert result.status == StageStatus.SKIPPED
    assert result.context is context
    assert result.metadata == {"job_outcome": existing_outcome}
    assert context.metadata["job_outcome"] is existing_outcome
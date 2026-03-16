from __future__ import annotations

from pathlib import Path

from core.job.job import Job
from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages.workspace_stage import WorkspaceStage, WorkspaceStageConfig


def test_workspace_stage_creates_workspace_and_updates_context(tmp_path: Path) -> None:
    context = JobContext(
        job_id="job-123",
        request={"job_id": "job-123", "input_uri": "s3://bucket/file.mp4"},
        input_uri="s3://bucket/file.mp4",
    )

    stage = WorkspaceStage(
        WorkspaceStageConfig(base_dir=tmp_path / "jobs"),
    )

    result = stage.run(context)

    expected_root = tmp_path / "jobs" / "job-123"

    assert context.workspace_dir == expected_root
    assert expected_root.exists()
    assert (expected_root / "input").exists()
    assert (expected_root / "audio").exists()
    assert (expected_root / "output").exists()
    assert (expected_root / "logs").exists()

    assert "job" in context.metadata
    assert isinstance(context.metadata["job"], Job)
    assert context.metadata["job"].job_id == "job-123"

    assert "workspace_paths" in context.metadata
    assert result.status == StageStatus.SUCCESS


def test_workspace_stage_is_idempotent(tmp_path: Path) -> None:
    base_dir = tmp_path / "jobs"

    existing_job = Job(job_id="job-123", base_dir=str(base_dir))
    existing_root = Path(existing_job.root)

    context = JobContext(
        job_id="job-123",
        request={"job_id": "job-123"},
    )

    stage = WorkspaceStage(
        WorkspaceStageConfig(base_dir=base_dir),
    )

    result = stage.run(context)

    assert context.workspace_dir == existing_root
    assert existing_root.exists()
    assert (existing_root / "input").exists()
    assert (existing_root / "audio").exists()
    assert (existing_root / "output").exists()
    assert (existing_root / "logs").exists()

    assert "job" in context.metadata
    assert isinstance(context.metadata["job"], Job)
    assert context.metadata["job"].job_id == "job-123"

    assert result.status == StageStatus.SUCCESS
# tests/pipeline/stages/test_cleanup_stage.py

from __future__ import annotations

from pathlib import Path

import pytest

from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages.cleanup_stage import CleanupStage


def test_cleanup_stage_removes_workspace_and_updates_context(tmp_path):
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    (workspace_dir / "input.mp4").write_bytes(b"video")
    (workspace_dir / "temp").mkdir()
    (workspace_dir / "temp" / "audio.wav").write_bytes(b"audio")

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"workspace_dir": str(workspace_dir)},
    )

    stage = CleanupStage()
    result = stage.run(context)

    assert result.status == StageStatus.SUCCESS
    assert result.context is context
    assert not workspace_dir.exists()
    assert context.metadata["cleanup"] == {
        "workspace_dir": str(workspace_dir),
        "removed": True,
    }
    assert result.metadata == {
        "cleanup": {
            "workspace_dir": str(workspace_dir),
            "removed": True,
        }
    }


def test_cleanup_stage_succeeds_when_workspace_already_missing(tmp_path):
    workspace_dir = tmp_path / "workspace"

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"workspace_dir": str(workspace_dir)},
    )

    stage = CleanupStage()
    result = stage.run(context)

    assert result.status == StageStatus.SUCCESS
    assert context.metadata["cleanup"] == {
        "workspace_dir": str(workspace_dir),
        "removed": False,
    }
    assert result.metadata == {
        "cleanup": {
            "workspace_dir": str(workspace_dir),
            "removed": False,
        }
    }


def test_cleanup_stage_raises_when_workspace_dir_missing():
    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={},
    )

    stage = CleanupStage()

    with pytest.raises(
        ValueError,
        match=r"context\.metadata\['workspace_dir'\] is required for CleanupStage",
    ):
        stage.run(context)


def test_cleanup_stage_skips_when_cleanup_already_exists(tmp_path):
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()

    existing_cleanup = {
        "workspace_dir": str(workspace_dir),
        "removed": True,
    }

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={
            "workspace_dir": str(workspace_dir),
            "cleanup": existing_cleanup,
        },
    )

    stage = CleanupStage()
    result = stage.run(context)

    assert result.status == StageStatus.SKIPPED
    assert result.context is context
    assert result.metadata == {"cleanup": existing_cleanup}
    assert context.metadata["cleanup"] is existing_cleanup
    assert workspace_dir.exists()
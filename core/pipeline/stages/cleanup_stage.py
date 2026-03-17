# core/pipeline/stages/cleanup_stage.py

from __future__ import annotations

import shutil
from pathlib import Path

from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


class CleanupStage(Stage):
    """
    Removes local workspace files after processing completes.

    Reads:
    - context.metadata["workspace_dir"]

    Writes:
    - context.metadata["cleanup"]
    """

    name = "cleanup"

    def run(self, context: JobContext) -> StageResult:
        existing_cleanup = context.metadata.get("cleanup")
        if existing_cleanup is not None:
            return StageResult(
                status=StageStatus.SKIPPED,
                context=context,
                metadata={"cleanup": existing_cleanup},
            )

        workspace_dir_value = context.metadata.get("workspace_dir")
        if not workspace_dir_value:
            raise ValueError(
                "context.metadata['workspace_dir'] is required for CleanupStage"
            )

        workspace_dir = Path(workspace_dir_value)

        removed = False
        if workspace_dir.exists():
            shutil.rmtree(workspace_dir)
            removed = True

        cleanup = {
            "workspace_dir": str(workspace_dir),
            "removed": removed,
        }

        context.metadata["cleanup"] = cleanup

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
            metadata={"cleanup": cleanup},
        )
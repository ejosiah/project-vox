from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from core.job.job import Job
from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


@dataclass(slots=True)
class WorkspaceStageConfig:
    base_dir: str | Path = "jobs"


class WorkspaceStage(Stage):
    """
    Create the job workspace on disk and attach it to the JobContext.
    """

    name = "workspace"

    def __init__(self, config: WorkspaceStageConfig | None = None) -> None:
        self._config = config or WorkspaceStageConfig()

    def run(self, context: JobContext) -> StageResult:
        if not context.job_id:
            raise ValueError("context.job_id is required for WorkspaceStage")

        job = Job(
            job_id=context.job_id,
            base_dir=str(self._config.base_dir),
        )

        # set workspace path on context
        context.set_workspace(Path(job.root))

        # store Job object for later stages
        context.add_metadata("job", job)
        context.add_metadata("workspace_paths", job.paths())

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
            metadata={
                "job_id": context.job_id,
                "workspace_dir": str(context.workspace_dir),
            },
        )
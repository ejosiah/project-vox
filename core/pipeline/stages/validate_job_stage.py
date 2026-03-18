from __future__ import annotations

from typing import Iterable

from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


class ValidateJobStage(Stage):
    name = "validate"

    def __init__(self, required_fields: Iterable[str] | None = None) -> None:
        self._required_fields = tuple(required_fields or ("job_id", "source"))

    def run(self, context: JobContext) -> StageResult:
        request = context.request

        if request is None:
            context.set_error("Job request payload missing.")
            return StageResult(
                status=StageStatus.FAILED,
                context=context,
            )

        missing_fields = [
            field for field in self._required_fields if field not in request
        ]
        if missing_fields:
            context.set_error(
                f"Missing required job fields: {', '.join(missing_fields)}"
            )
            return StageResult(
                status=StageStatus.FAILED,
                context=context,
            )

        if not context.job_id:
            context.set_error("Invalid or empty job_id")
            return StageResult(
                status=StageStatus.FAILED,
                context=context,
            )

        source = request.get("source")
        if not source:
            context.set_error("source is required")
            return StageResult(
                status=StageStatus.FAILED,
                context=context,
            )

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
        )
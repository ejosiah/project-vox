# core/pipeline/stages/finalize_job_stage.py

from __future__ import annotations

from datetime import datetime, timezone

from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


class FinalizeJobStage(Stage):
    """
    Marks the job as finalized so the worker can publish completion metadata.

    Reads:
    - context.job_id
    - context.metadata["uploaded_outputs"] (optional)
    - context.metadata["generated_outputs"] (optional)

    Writes:
    - context.metadata["job_outcome"]
    """

    name = "finalize_job"

    def run(self, context: JobContext) -> StageResult:
        existing_job_outcome = context.metadata.get("job_outcome")
        if existing_job_outcome is not None:
            return StageResult(
                status=StageStatus.SKIPPED,
                context=context,
                metadata={"job_outcome": existing_job_outcome},
            )

        uploaded_outputs = context.metadata.get("uploaded_outputs")
        generated_outputs = context.metadata.get("generated_outputs")

        output_count = 0
        if uploaded_outputs is not None and hasattr(uploaded_outputs, "outputs"):
            output_count = len(uploaded_outputs.outputs)
        elif isinstance(generated_outputs, dict):
            output_count = len(generated_outputs)

        job_outcome = {
            "job_id": context.job_id,
            "status": "success",
            "finalized_at": datetime.now(timezone.utc).isoformat(),
            "output_count": output_count,
            "has_uploaded_outputs": uploaded_outputs is not None,
            "has_generated_outputs": generated_outputs is not None,
        }

        context.metadata["job_outcome"] = job_outcome

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
            metadata={"job_outcome": job_outcome},
        )
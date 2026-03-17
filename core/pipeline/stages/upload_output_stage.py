# core/pipeline/stages/upload_output_stage.py

from __future__ import annotations

from typing import Any

from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


class UploadOutputStage(Stage):
    """
    Uploads generated output files using the configured output uploader.

    Reads:
    - context.job_id
    - context.metadata["generated_outputs"]

    Writes:
    - context.metadata["uploaded_outputs"]
    """

    name = "upload_output"

    def __init__(self, uploader: Any) -> None:
        self._uploader = uploader

    def run(self, context: JobContext) -> StageResult:
        generated_outputs = context.metadata.get("generated_outputs")
        if generated_outputs is None:
            raise ValueError(
                "context.metadata['generated_outputs'] is required for UploadOutputStage"
            )

        existing_uploaded_outputs = context.metadata.get("uploaded_outputs")
        if existing_uploaded_outputs is not None:
            return StageResult(
                status=StageStatus.SKIPPED,
                context=context,
                metadata={"uploaded_outputs": existing_uploaded_outputs},
            )

        upload_result = self._uploader.upload(
            job_id=context.job_id,
            outputs=generated_outputs,
        )

        context.metadata["uploaded_outputs"] = upload_result

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
            metadata={"uploaded_outputs": upload_result},
        )
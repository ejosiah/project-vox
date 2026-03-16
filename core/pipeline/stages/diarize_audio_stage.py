# core/pipeline/stages/diarize_audio_stage.py

from __future__ import annotations

from pathlib import Path
from typing import Any

from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


class DiarizeAudioStage(Stage):
    """
    Runs speaker diarization on normalized audio.

    Reads:
    - context.metadata["normalized_audio_path"]

    Writes:
    - context.metadata["diarization"]
    """

    name = "diarize_audio"

    def __init__(self, diarizer: Any) -> None:
        self._diarizer = diarizer

    def run(self, context: JobContext) -> StageResult:
        normalized_audio_path = context.metadata.get("normalized_audio_path")
        if not normalized_audio_path:
            raise ValueError(
                "context.metadata['normalized_audio_path'] is required for DiarizeAudioStage"
            )

        existing_diarization = context.metadata.get("diarization")
        if existing_diarization is not None:
            return StageResult(
                status=StageStatus.SKIPPED,
                context=context,
                metadata={"diarization": existing_diarization},
            )

        audio_path = Path(normalized_audio_path)
        diarization = self._diarizer.diarize(audio_path)

        context.metadata["diarization"] = diarization

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
            metadata={"diarization": diarization},
        )
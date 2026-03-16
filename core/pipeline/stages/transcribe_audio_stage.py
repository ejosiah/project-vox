# core/pipeline/stages/transcribe_audio_stage.py

from __future__ import annotations

from pathlib import Path
from typing import Any

from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


class TranscribeAudioStage(Stage):
    """
    Transcribes normalized audio into structured transcript output.

    Reads:
    - context.metadata["normalized_audio_path"]

    Writes:
    - context.metadata["transcript"]
    """

    name = "transcribe_audio"

    def __init__(self, transcriber: Any) -> None:
        self._transcriber = transcriber

    def run(self, context: JobContext) -> StageResult:
        normalized_audio_path = context.metadata.get("normalized_audio_path")
        if not normalized_audio_path:
            raise ValueError(
                "context.metadata['normalized_audio_path'] is required for TranscribeAudioStage"
            )

        existing_transcript = context.metadata.get("transcript")
        if existing_transcript is not None:
            return StageResult(
                status=StageStatus.SKIPPED,
                context=context,
                metadata={"transcript": existing_transcript},
            )

        audio_path = Path(normalized_audio_path)
        transcript = self._transcriber.transcribe(audio_path)

        context.metadata["transcript"] = transcript

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
            metadata={"transcript": transcript},
        )
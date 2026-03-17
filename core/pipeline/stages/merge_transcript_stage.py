# core/pipeline/stages/merge_transcript_stage.py

from __future__ import annotations

from dataclasses import replace
from typing import Any

from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


class MergeTranscriptStage(Stage):
    """
    Merges adjacent aligned transcript segments that belong to the same speaker.

    Reads:
    - context.metadata["aligned_transcript"]

    Writes:
    - context.metadata["merged_transcript"]
    """

    name = "merge_transcript"

    def run(self, context: JobContext) -> StageResult:
        aligned_transcript = context.metadata.get("aligned_transcript")
        if aligned_transcript is None:
            raise ValueError(
                "context.metadata['aligned_transcript'] is required for MergeTranscriptStage"
            )

        existing_merged_transcript = context.metadata.get("merged_transcript")
        if existing_merged_transcript is not None:
            return StageResult(
                status=StageStatus.SKIPPED,
                context=context,
                metadata={"merged_transcript": existing_merged_transcript},
            )

        merged_transcript = self._merge_transcript(aligned_transcript)
        context.metadata["merged_transcript"] = merged_transcript

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
            metadata={"merged_transcript": merged_transcript},
        )

    def _merge_transcript(self, aligned_transcript: Any) -> Any:
        source_segments = list(getattr(aligned_transcript, "segments", []))
        if not source_segments:
            return replace(aligned_transcript, segments=[])

        merged_segments: list[Any] = []
        current = source_segments[0]

        for segment in source_segments[1:]:
            if self._can_merge(current, segment):
                current = self._merge_two_segments(current, segment)
            else:
                merged_segments.append(current)
                current = segment

        merged_segments.append(current)

        merged_text = " ".join(
            str(getattr(segment, "text", "")).strip()
            for segment in merged_segments
            if str(getattr(segment, "text", "")).strip()
        ).strip()

        return replace(
            aligned_transcript,
            text=merged_text,
            segments=merged_segments,
        )

    @staticmethod
    def _can_merge(left: Any, right: Any) -> bool:
        return getattr(left, "speaker", None) == getattr(right, "speaker", None)

    @staticmethod
    def _merge_two_segments(left: Any, right: Any) -> Any:
        left_text = str(getattr(left, "text", "")).strip()
        right_text = str(getattr(right, "text", "")).strip()

        merged_text = " ".join(part for part in (left_text, right_text) if part).strip()

        left_words = list(getattr(left, "words", []))
        right_words = list(getattr(right, "words", []))
        merged_words = [*left_words, *right_words]

        return replace(
            left,
            end=float(getattr(right, "end")),
            text=merged_text,
            words=merged_words,
        )
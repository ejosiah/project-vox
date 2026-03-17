# core/pipeline/stages/align_transcript_stage.py

from __future__ import annotations

from dataclasses import replace
from typing import Any

from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


class AlignTranscriptStage(Stage):
    """
    Aligns transcript segments with diarization speaker turns using max overlap.

    Reads:
    - context.metadata["transcript"]
    - context.metadata["diarization"]

    Writes:
    - context.metadata["aligned_transcript"]
    """

    name = "align_transcript"

    def run(self, context: JobContext) -> StageResult:
        transcript = context.metadata.get("transcript")
        if transcript is None:
            raise ValueError(
                "context.metadata['transcript'] is required for AlignTranscriptStage"
            )

        diarization = context.metadata.get("diarization")
        if diarization is None:
            raise ValueError(
                "context.metadata['diarization'] is required for AlignTranscriptStage"
            )

        existing_aligned_transcript = context.metadata.get("aligned_transcript")
        if existing_aligned_transcript is not None:
            return StageResult(
                status=StageStatus.SKIPPED,
                context=context,
                metadata={"aligned_transcript": existing_aligned_transcript},
            )

        aligned_transcript = self._align_transcript(transcript, diarization)

        context.metadata["aligned_transcript"] = aligned_transcript

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
            metadata={"aligned_transcript": aligned_transcript},
        )

    def _align_transcript(self, transcript: Any, diarization: Any) -> Any:
        diarization_segments = sorted(
            list(getattr(diarization, "segments", [])),
            key=lambda seg: float(seg.start),
        )

        aligned_segments = []
        for segment in getattr(transcript, "segments", []):
            speaker = self._pick_speaker_for_segment(
                diarization_segments,
                float(segment.start),
                float(segment.end),
            )
            aligned_segments.append(replace(segment, speaker=speaker))

        return replace(transcript, segments=aligned_segments)

    def _pick_speaker_for_segment(
        self,
        diarization_segments: list[Any],
        start: float,
        end: float,
    ) -> str:
        best_speaker = "UNKNOWN"
        best_overlap = 0.0

        for turn in diarization_segments:
            turn_start = float(turn.start)
            turn_end = float(turn.end)

            if turn_start >= end:
                break
            if turn_end <= start:
                continue

            current_overlap = self._overlap(start, end, turn_start, turn_end)
            if current_overlap > best_overlap:
                best_overlap = current_overlap
                best_speaker = str(turn.speaker)

        return best_speaker

    @staticmethod
    def _overlap(a0: float, a1: float, b0: float, b1: float) -> float:
        return max(0.0, min(a1, b1) - max(a0, b0))
# core/media/diarizer.py

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyannote.core import Annotation

from core.media.diarizer_backend import PyannoteDiarizerBackend


@dataclass(slots=True)
class SpeakerSegment:
    start: float
    end: float
    speaker: str


@dataclass(slots=True)
class DiarizationResult:
    segments: list[SpeakerSegment] = field(default_factory=list)
    annotation: Annotation | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "segments": [
                {
                    "start": item.start,
                    "end": item.end,
                    "speaker": item.speaker,
                }
                for item in self.segments
            ],
            "raw": self.raw,
        }


class DiarizerError(Exception):
    pass


class Diarizer:
    def __init__(self, backend: PyannoteDiarizerBackend) -> None:
        self._backend = backend

    def diarize(self, audio_path: str | Path) -> DiarizationResult:
        path = Path(audio_path)
        if not path.exists():
            raise DiarizerError(f"Audio file does not exist: {path}")

        raw_result = self._backend.diarize(path)
        return self._normalize_result(raw_result)

    @staticmethod
    def _normalize_result(raw_result: dict[str, Any]) -> DiarizationResult:
        if not isinstance(raw_result, dict):
            raise DiarizerError("Diarizer backend must return a dict")

        segments_data = raw_result.get("segments", [])
        if not isinstance(segments_data, list):
            raise DiarizerError("Diarizer result 'segments' must be a list")

        segments: list[SpeakerSegment] = []
        for item in segments_data:
            if not isinstance(item, dict):
                raise DiarizerError("Each diarization segment must be a dict")

            speaker = str(item.get("speaker", "")).strip()
            if not speaker:
                raise DiarizerError("Each diarization segment must include a speaker")

            segments.append(
                SpeakerSegment(
                    start=float(item.get("start", 0.0)),
                    end=float(item.get("end", 0.0)),
                    speaker=speaker,
                )
            )

        return DiarizationResult(
            segments=segments,
            annotation=raw_result.get("annotation"),
            raw=raw_result,
        )
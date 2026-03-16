# core/media/transcriber.py

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.media.transcriber_backend import FasterWhisperTranscriberBackend


@dataclass(slots=True)
class TranscriptSegment:
    start: float
    end: float
    text: str
    speaker: str | None = None
    words: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class TranscriptResult:
    text: str
    language: str | None = None
    language_probability: float | None = None
    segments: list[TranscriptSegment] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "language": self.language,
            "language_probability": self.language_probability,
            "segments": [
                {
                    "start": item.start,
                    "end": item.end,
                    "text": item.text,
                    "speaker": item.speaker,
                    "words": item.words,
                }
                for item in self.segments
            ],
            "raw": self.raw,
        }


class TranscriberError(Exception):
    pass


class Transcriber:
    def __init__(self, backend: FasterWhisperTranscriberBackend) -> None:
        self._backend = backend

    def transcribe(self, audio_path: str | Path) -> TranscriptResult:
        path = Path(audio_path)
        if not path.exists():
            raise TranscriberError(f"Audio file does not exist: {path}")

        raw_result = self._backend.transcribe(path)
        return self._normalize_result(raw_result)

    @staticmethod
    def _normalize_result(raw_result: dict[str, Any]) -> TranscriptResult:
        if not isinstance(raw_result, dict):
            raise TranscriberError("Transcriber backend must return a dict")

        segments_data = raw_result.get("segments", [])
        if not isinstance(segments_data, list):
            raise TranscriberError("Transcriber result 'segments' must be a list")

        segments: list[TranscriptSegment] = []
        full_text_parts: list[str] = []

        for item in segments_data:
            if not isinstance(item, dict):
                raise TranscriberError("Each transcript segment must be a dict")

            text = str(item.get("text", "")).strip()
            if not text:
                continue

            full_text_parts.append(text)
            segments.append(
                TranscriptSegment(
                    start=float(item.get("start", 0.0)),
                    end=float(item.get("end", 0.0)),
                    text=text,
                    speaker=item.get("speaker"),
                    words=list(item.get("words", [])),
                )
            )

        text = str(raw_result.get("text", "")).strip() or " ".join(full_text_parts).strip()

        return TranscriptResult(
            text=text,
            language=raw_result.get("language"),
            language_probability=raw_result.get("language_probability"),
            segments=segments,
            raw=raw_result,
        )
# core/media/transcriber_backend.py

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import torch
from faster_whisper import WhisperModel


@dataclass(slots=True)
class WhisperTranscriptionInfo:
    language: str | None
    language_probability: float | None


class FasterWhisperTranscriberBackend:
    def __init__(
        self,
        *,
        model_name: str = "base",
        prefer_gpu: bool = True,
        enable_tf32: bool = True,
        force_language: str | None = None,
        compute_type_cpu: str = "int8",
        compute_type_cuda: str = "float16",
        vad_filter: bool = True,
    ) -> None:
        self._model_name = model_name
        self._prefer_gpu = prefer_gpu
        self._enable_tf32 = enable_tf32
        self._force_language = force_language
        self._compute_type_cpu = compute_type_cpu
        self._compute_type_cuda = compute_type_cuda
        self._vad_filter = vad_filter

        self._device = self._resolve_device()
        self._configure_torch()
        self._model = WhisperModel(
            self._model_name,
            device=self._device,
            compute_type=self._compute_type(),
        )

    @property
    def device(self) -> str:
        return self._device

    def transcribe(self, audio_path: str | Path) -> dict[str, Any]:
        path = Path(audio_path)

        segments, info = self._model.transcribe(
            str(path),
            language=(self._force_language if self._force_language else None),
            vad_filter=self._vad_filter,
        )

        normalized_segments: list[dict[str, Any]] = []
        full_text_parts: list[str] = []

        for seg in segments:
            text = (seg.text or "").strip()
            if not text:
                continue

            full_text_parts.append(text)
            normalized_segments.append(
                {
                    "start": float(seg.start),
                    "end": float(seg.end),
                    "text": text,
                    "words": [],
                }
            )

        return {
            "text": " ".join(full_text_parts).strip(),
            "language": getattr(info, "language", None),
            "language_probability": getattr(info, "language_probability", None),
            "segments": normalized_segments,
            "raw_info": WhisperTranscriptionInfo(
                language=getattr(info, "language", None),
                language_probability=getattr(info, "language_probability", None),
            ),
        }

    def _resolve_device(self) -> str:
        has_cuda = torch.cuda.is_available()
        return "cuda" if (self._prefer_gpu and has_cuda) else "cpu"

    def _compute_type(self) -> str:
        return self._compute_type_cuda if self._device == "cuda" else self._compute_type_cpu

    def _configure_torch(self) -> None:
        if self._device == "cuda" and self._enable_tf32:
            torch.backends.cuda.matmul.allow_tf32 = True
            torch.backends.cudnn.allow_tf32 = True
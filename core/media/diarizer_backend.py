# core/media/diarizer_backend.py

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import torch
from pyannote.audio import Pipeline
from pyannote.core import Annotation


class PyannoteDiarizerBackend:
    def __init__(
        self,
        *,
        token_env_var: str = "HF_TOKEN",
        model_name: str = "pyannote/speaker-diarization-3.1",
        prefer_gpu: bool = True,
        enable_tf32: bool = True,
        num_speakers: int = 0,
        min_speakers: int = 0,
        max_speakers: int = 0,
    ) -> None:
        token = os.getenv(token_env_var)
        if not token:
            raise RuntimeError(
                f"Missing Hugging Face token. Set {token_env_var}=hf_..."
            )

        self._prefer_gpu = prefer_gpu
        self._enable_tf32 = enable_tf32
        self._num_speakers = num_speakers
        self._min_speakers = min_speakers
        self._max_speakers = max_speakers

        self._device = self._resolve_device()
        self._configure_torch()

        self._pipeline = Pipeline.from_pretrained(model_name, token=token)
        self._pipeline.to(torch.device(self._device))

    @property
    def device(self) -> str:
        return self._device

    def diarize(self, audio_path: str | Path) -> dict[str, Any]:
        path = Path(audio_path)

        diarization_output = self._pipeline(str(path), **self._diarize_kwargs())
        annotation = self.extract_annotation(diarization_output)

        segments: list[dict[str, Any]] = []
        for segment, _, speaker in annotation.itertracks(yield_label=True):
            segments.append(
                {
                    "start": float(segment.start),
                    "end": float(segment.end),
                    "speaker": str(speaker),
                }
            )

        segments.sort(key=lambda item: item["start"])

        return {
            "segments": segments,
            "annotation": annotation,
        }

    def _resolve_device(self) -> str:
        has_cuda = torch.cuda.is_available()
        return "cuda" if (self._prefer_gpu and has_cuda) else "cpu"

    def _configure_torch(self) -> None:
        if self._device == "cuda" and self._enable_tf32:
            torch.backends.cuda.matmul.allow_tf32 = True
            torch.backends.cudnn.allow_tf32 = True

    def _diarize_kwargs(self) -> dict[str, int]:
        if self._num_speakers > 0:
            return {"num_speakers": self._num_speakers}

        kwargs: dict[str, int] = {}
        if self._min_speakers > 0:
            kwargs["min_speakers"] = self._min_speakers
        if self._max_speakers > 0:
            kwargs["max_speakers"] = self._max_speakers
        return kwargs

    @staticmethod
    def extract_annotation(diarization_output: Any) -> Annotation:
        if isinstance(diarization_output, Annotation):
            return diarization_output

        for attr in ("annotation", "diarization", "predicted_labels", "labels"):
            if hasattr(diarization_output, attr):
                value = getattr(diarization_output, attr)
                if isinstance(value, Annotation):
                    return value

        if hasattr(diarization_output, "__getitem__"):
            for key in ("annotation", "diarization"):
                try:
                    value = diarization_output[key]
                except Exception:
                    continue
                if isinstance(value, Annotation):
                    return value

        for name in dir(diarization_output):
            if name.startswith("_"):
                continue
            try:
                value = getattr(diarization_output, name)
            except Exception:
                continue
            if isinstance(value, Annotation):
                return value

        raise TypeError(
            "Could not extract pyannote.core.Annotation from diarization output. "
            f"Got type: {type(diarization_output)}"
        )
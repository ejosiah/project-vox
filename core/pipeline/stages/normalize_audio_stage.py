# core/pipeline/stages/normalize_audio_stage.py

from __future__ import annotations

from pathlib import Path

from core.job.job import Job
from core.media.ffmpeg_utils import FFmpeg
from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


class NormalizeAudioStage(Stage):
    """
    Normalize audio into the transcription engine's required format.

    Default output format:
    - mono
    - 16 kHz
    - PCM s16le WAV

    Expects an existing audio probe in context.metadata["media_probe"].
    """

    name = "normalize_audio"

    def __init__(
        self,
        ffmpeg: FFmpeg | None = None,
        *,
        sample_rate: int = 16000,
        channels: int = 1,
        codec_name: str = "pcm_s16le",
    ) -> None:
        self._ffmpeg = ffmpeg or FFmpeg()
        self._sample_rate = sample_rate
        self._channels = channels
        self._codec_name = codec_name

    def run(self, context: JobContext) -> StageResult:
        if context.audio_path is None:
            raise ValueError("context.audio_path is required for NormalizeAudioStage")

        probe = context.metadata.get("media_probe")
        if probe is None:
            raise ValueError(
                "context.metadata['media_probe'] is required for NormalizeAudioStage"
            )

        source_path = Path(context.audio_path)
        audio_stream = self._get_audio_stream(probe)

        if self._is_already_normalized(source_path, audio_stream):
            context.add_metadata("normalized_audio_probe", probe)
            return StageResult(
                status=StageStatus.SKIPPED,
                context=context,
                metadata={
                    "job_id": context.job_id,
                    "audio_path": str(source_path),
                    "normalized_audio_path": str(source_path),
                    "sample_rate": self._sample_rate,
                    "channels": self._channels,
                    "codec_name": self._codec_name,
                },
            )

        output_dir = self._resolve_audio_dir(context)
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = self._build_output_path(source_path, output_dir)
        normalized_path = self._ffmpeg.convert_to_wav(
            source_path,
            output_path,
            sample_rate=self._sample_rate,
            channels=self._channels,
        )

        context.audio_path = normalized_path
        context.add_metadata(
            "normalized_audio_probe",
            {
                "streams": [
                    {
                        "codec_type": "audio",
                        "codec_name": self._codec_name,
                        "sample_rate": str(self._sample_rate),
                        "channels": self._channels,
                    }
                ]
            },
        )

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
            metadata={
                "job_id": context.job_id,
                "audio_path": str(source_path),
                "normalized_audio_path": str(normalized_path),
                "sample_rate": self._sample_rate,
                "channels": self._channels,
                "codec_name": self._codec_name,
            },
        )

    @staticmethod
    def _resolve_audio_dir(context: JobContext) -> Path:
        job = context.metadata.get("job")
        if isinstance(job, Job):
            return Path(job.audio_dir)

        if context.workspace_dir is None:
            raise ValueError(
                "context.workspace_dir is required when no Job is present in metadata"
            )

        return Path(context.workspace_dir) / "audio"

    @staticmethod
    def _get_audio_stream(probe: dict) -> dict:
        streams = probe.get("streams", [])
        for stream in streams:
            if stream.get("codec_type") == "audio":
                return stream

        raise ValueError("Audio probe does not contain an audio stream")

    def _is_already_normalized(self, audio_path: Path, stream: dict) -> bool:
        codec_name = stream.get("codec_name")
        sample_rate = stream.get("sample_rate")
        channels = stream.get("channels")

        return (
            audio_path.suffix.lower() == ".wav"
            and codec_name == self._codec_name
            and self._safe_int(sample_rate) == self._sample_rate
            and self._safe_int(channels) == self._channels
        )

    @staticmethod
    def _safe_int(value: object) -> int | None:
        if value is None:
            return None
        return int(value)

    @staticmethod
    def _build_output_path(source_path: Path, output_dir: Path) -> Path:
        if source_path.suffix.lower() == ".wav":
            return output_dir / f"{source_path.stem}_normalized.wav"

        return output_dir / f"{source_path.stem}.wav"
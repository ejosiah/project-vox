# core/pipeline/stages/extract_audio_stage.py

from __future__ import annotations

import shutil
from pathlib import Path

from core.media.ffmpeg_utils import FFmpeg
from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


TRANSCRIPTION_FRIENDLY_EXTENSIONS = {".wav"}


class ExtractAudioStage(Stage):
    """
    Extract or normalize audio into the workspace audio directory.

    Rules:
    - audio-only input:
        - copy directly if already transcription-friendly
        - otherwise convert to wav
    - media with video and audio:
        - extract audio to wav
    - video with no audio:
        - fail
    - no usable streams:
        - fail
    """

    name = "extract_audio"

    def __init__(
        self,
        ffmpeg: FFmpeg | None = None,
        *,
        sample_rate: int = 16000,
        channels: int = 1,
    ) -> None:
        self._ffmpeg = ffmpeg or FFmpeg()
        self._sample_rate = sample_rate
        self._channels = channels

    def run(self, context: JobContext) -> StageResult:
        if context.input_path is None:
            raise ValueError("context.input_path is required for ExtractAudioStage")

        probe = context.metadata.get("media_probe")
        if probe is None:
            raise ValueError(
                "context.metadata['media_probe'] is required for ExtractAudioStage"
            )

        input_path = Path(context.input_path)
        media_type = self._classify_media(probe)

        output_dir = self._resolve_audio_dir(context)
        output_dir.mkdir(parents=True, exist_ok=True)

        if media_type == "audio":
            if input_path.suffix.lower() in TRANSCRIPTION_FRIENDLY_EXTENSIONS:
                output_path = output_dir / input_path.name
                shutil.copy2(input_path, output_path)
            else:
                output_path = output_dir / f"{input_path.stem}.wav"
                output_path = self._ffmpeg.convert_to_wav(
                    input_path,
                    output_path,
                    sample_rate=self._sample_rate,
                    channels=self._channels,
                )
        else:
            output_path = output_dir / f"{input_path.stem}.wav"
            output_path = self._ffmpeg.extract_audio(input_path, output_path)

        context.audio_path = output_path

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
            metadata={
                "job_id": context.job_id,
                "input_path": str(input_path),
                "audio_path": str(output_path),
                "media_type": media_type,
            },
        )

    @staticmethod
    def _resolve_audio_dir(context: JobContext) -> Path:
        job = context.metadata.get("job")

        if job is not None and hasattr(job, "audio_dir"):
            return Path(job.audio_dir)

        if context.workspace_dir is None:
            raise ValueError(
                "context.workspace_dir is required when no Job is present in metadata"
            )

        return Path(context.workspace_dir) / "audio"

    @staticmethod
    def _classify_media(probe: dict) -> str:
        streams = probe.get("streams", [])

        has_audio = any(stream.get("codec_type") == "audio" for stream in streams)
        has_video = any(stream.get("codec_type") == "video" for stream in streams)

        if has_audio and not has_video:
            return "audio"

        if has_audio and has_video:
            return "video"

        if has_video and not has_audio:
            raise ValueError("Input media contains video streams but no audio streams")

        raise ValueError("Input media contains no usable audio or video streams")
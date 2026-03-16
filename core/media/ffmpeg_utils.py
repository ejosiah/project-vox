from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from core.utils.command import CommandRunnerMixin
from core.utils.binary import BinaryValidationMixin
from core.utils.command import CommandError


class FFmpeg(CommandRunnerMixin, BinaryValidationMixin):
    def __init__(self, ffmpeg_bin: str = "ffmpeg", ffprobe_bin: str = "ffprobe"):
        self.ffmpeg_bin = ffmpeg_bin
        self.ffprobe_bin = ffprobe_bin

        self.validate_binary(self.ffmpeg_bin)
        self.validate_binary(self.ffprobe_bin)

    def probe_media(self, path: str | Path) -> dict[str, Any]:

        media_path = Path(path)

        if not media_path.exists():
            raise CommandError(f"Media file does not exist: {media_path}")

        command = [
            self.ffprobe_bin,
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            str(media_path),
        ]

        result = self.run_command(command)

        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise CommandError(f"Failed to parse ffprobe output for file: {media_path}") from exc

    def extract_audio(self, input_path: str | Path, output_path: str | Path) -> Path:

        src = Path(input_path)
        dst = Path(output_path)

        if not src.exists():
            raise CommandError(f"Input file does not exist: {src}")

        dst.parent.mkdir(parents=True, exist_ok=True)

        command = [
            self.ffmpeg_bin,
            "-y",
            "-i",
            str(src),
            "-vn",
            "-acodec",
            "pcm_s16le",
            str(dst),
        ]

        self.run_command(command)
        return dst

    def convert_to_wav(
        self,
        input_path: str | Path,
        output_path: str | Path,
        sample_rate: int = 16000,
        channels: int = 1,
    ) -> Path:

        src = Path(input_path)
        dst = Path(output_path)

        if not src.exists():
            raise CommandError(f"Input file does not exist: {src}")

        dst.parent.mkdir(parents=True, exist_ok=True)

        command = [
            self.ffmpeg_bin,
            "-y",
            "-i",
            str(src),
            "-vn",
            "-ac",
            str(channels),
            "-ar",
            str(sample_rate),
            "-c:a",
            "pcm_s16le",
            str(dst),
        ]

        self.run_command(command)

        return dst

    def get_audio_duration(self, path: str | Path) -> float:

        metadata = self.probe_media(path)

        format_info = metadata.get("format", {})
        duration = format_info.get("duration")

        if duration is None:
            raise CommandError(f"Could not determine media duration for: {path}")

        return float(duration)

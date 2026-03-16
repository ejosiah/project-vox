from __future__ import annotations

from pathlib import Path

import pytest

from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages.probe_media_stage import ProbeMediaStage
from core.utils.command import CommandError


class FakeFFmpeg:
    def __init__(self, probe_result: dict | None = None, error: Exception | None = None):
        self.probe_result = probe_result or {}
        self.error = error
        self.calls: list[Path] = []

    def probe_media(self, path: str | Path) -> dict:
        media_path = Path(path)
        self.calls.append(media_path)

        if self.error is not None:
            raise self.error

        return self.probe_result


def test_probe_media_stage_probes_input_and_updates_context(tmp_path: Path) -> None:
    input_file = tmp_path / "input.mp4"
    input_file.write_bytes(b"media-bytes")

    probe_result = {
        "format": {
            "filename": str(input_file),
            "duration": "12.345",
        },
        "streams": [
            {"codec_type": "video", "codec_name": "h264"},
            {"codec_type": "audio", "codec_name": "aac"},
        ],
    }
    ffmpeg = FakeFFmpeg(probe_result=probe_result)

    context = JobContext(
        job_id="job-123",
        request={"job_id": "job-123", "input_uri": str(input_file)},
        input_uri=str(input_file),
        input_path=input_file,
    )

    stage = ProbeMediaStage(ffmpeg=ffmpeg)

    result = stage.run(context)

    assert result.status == StageStatus.SUCCESS
    assert result.context is context
    assert ffmpeg.calls == [input_file]
    assert context.metadata["media_probe"] == probe_result
    assert result.metadata == {
        "job_id": "job-123",
        "input_path": str(input_file),
    }


def test_probe_media_stage_raises_when_input_path_missing() -> None:
    ffmpeg = FakeFFmpeg()

    context = JobContext(
        job_id="job-123",
        request={"job_id": "job-123", "input_uri": "example.mp4"},
        input_uri="example.mp4",
        input_path=None,
    )

    stage = ProbeMediaStage(ffmpeg=ffmpeg)

    with pytest.raises(ValueError, match="context.input_path is required"):
        stage.run(context)

    assert ffmpeg.calls == []


def test_probe_media_stage_propagates_probe_errors(tmp_path: Path) -> None:
    input_file = tmp_path / "broken.mp4"
    input_file.write_bytes(b"broken-media")

    ffmpeg = FakeFFmpeg(error=CommandError("ffprobe failed"))

    context = JobContext(
        job_id="job-999",
        request={"job_id": "job-999", "input_uri": str(input_file)},
        input_uri=str(input_file),
        input_path=input_file,
    )

    stage = ProbeMediaStage(ffmpeg=ffmpeg)

    with pytest.raises(CommandError, match="ffprobe failed"):
        stage.run(context)

    assert ffmpeg.calls == [input_file]
    assert "media_probe" not in context.metadata
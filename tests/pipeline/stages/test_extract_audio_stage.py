# tests/core/pipeline/stages/test_extract_audio_stage.py

from __future__ import annotations

from pathlib import Path

import pytest

from core.job.job import Job
from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages.extract_audio_stage import ExtractAudioStage


class FakeFFmpeg:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.extract_calls: list[dict[str, object]] = []
        self.convert_calls: list[dict[str, object]] = []

    def extract_audio(self, input_path: str | Path, output_path: str | Path) -> Path:
        call = {
            "input_path": Path(input_path),
            "output_path": Path(output_path),
        }
        self.extract_calls.append(call)

        if self.error is not None:
            raise self.error

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"extracted-wav")
        return output

    def convert_to_wav(
        self,
        input_path: str | Path,
        output_path: str | Path,
        sample_rate: int = 16000,
        channels: int = 1,
    ) -> Path:
        call = {
            "input_path": Path(input_path),
            "output_path": Path(output_path),
            "sample_rate": sample_rate,
            "channels": channels,
        }
        self.convert_calls.append(call)

        if self.error is not None:
            raise self.error

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"converted-wav")
        return output


def test_extract_audio_stage_extracts_audio_from_video_input(tmp_path: Path) -> None:
    input_file = tmp_path / "sample.mp4"
    input_file.write_bytes(b"video-data")

    job = Job(job_id="job-video", base_dir=str(tmp_path / "jobs"))
    ffmpeg = FakeFFmpeg()

    context = JobContext(
        job_id="job-video",
        request={"job_id": "job-video", "input_uri": str(input_file)},
        workspace_dir=Path(job.root),
        input_uri=str(input_file),
        input_path=input_file,
        metadata={
            "job": job,
            "media_probe": {
                "streams": [
                    {"codec_type": "video", "codec_name": "h264"},
                    {"codec_type": "audio", "codec_name": "aac"},
                ]
            },
        },
    )

    stage = ExtractAudioStage(ffmpeg=ffmpeg)

    result = stage.run(context)

    expected_audio_path = Path(job.audio_dir) / "sample.wav"

    assert result.status == StageStatus.SUCCESS
    assert result.context is context
    assert context.audio_path == expected_audio_path
    assert expected_audio_path.exists()
    assert expected_audio_path.read_bytes() == b"extracted-wav"
    assert ffmpeg.extract_calls == [
        {
            "input_path": input_file,
            "output_path": expected_audio_path,
        }
    ]
    assert ffmpeg.convert_calls == []
    assert result.metadata == {
        "job_id": "job-video",
        "input_path": str(input_file),
        "audio_path": str(expected_audio_path),
        "media_type": "video",
    }


def test_extract_audio_stage_copies_audio_only_wav_input(tmp_path: Path) -> None:
    input_file = tmp_path / "voice.wav"
    input_file.write_bytes(b"wav-data")

    job = Job(job_id="job-audio-copy", base_dir=str(tmp_path / "jobs"))
    ffmpeg = FakeFFmpeg()

    context = JobContext(
        job_id="job-audio-copy",
        request={"job_id": "job-audio-copy", "input_uri": str(input_file)},
        workspace_dir=Path(job.root),
        input_uri=str(input_file),
        input_path=input_file,
        metadata={
            "job": job,
            "media_probe": {
                "streams": [
                    {"codec_type": "audio", "codec_name": "pcm_s16le"},
                ]
            },
        },
    )

    stage = ExtractAudioStage(ffmpeg=ffmpeg)

    result = stage.run(context)

    expected_audio_path = Path(job.audio_dir) / "voice.wav"

    assert result.status == StageStatus.SUCCESS
    assert context.audio_path == expected_audio_path
    assert expected_audio_path.exists()
    assert expected_audio_path.read_bytes() == b"wav-data"
    assert ffmpeg.extract_calls == []
    assert ffmpeg.convert_calls == []
    assert result.metadata == {
        "job_id": "job-audio-copy",
        "input_path": str(input_file),
        "audio_path": str(expected_audio_path),
        "media_type": "audio",
    }


def test_extract_audio_stage_converts_audio_only_non_wav_input(tmp_path: Path) -> None:
    input_file = tmp_path / "voice.mp3"
    input_file.write_bytes(b"mp3-data")

    job = Job(job_id="job-audio-convert", base_dir=str(tmp_path / "jobs"))
    ffmpeg = FakeFFmpeg()

    context = JobContext(
        job_id="job-audio-convert",
        request={"job_id": "job-audio-convert", "input_uri": str(input_file)},
        workspace_dir=Path(job.root),
        input_uri=str(input_file),
        input_path=input_file,
        metadata={
            "job": job,
            "media_probe": {
                "streams": [
                    {"codec_type": "audio", "codec_name": "mp3"},
                ]
            },
        },
    )

    stage = ExtractAudioStage(ffmpeg=ffmpeg, sample_rate=16000, channels=1)

    result = stage.run(context)

    expected_audio_path = Path(job.audio_dir) / "voice.wav"

    assert result.status == StageStatus.SUCCESS
    assert context.audio_path == expected_audio_path
    assert expected_audio_path.exists()
    assert expected_audio_path.read_bytes() == b"converted-wav"
    assert ffmpeg.extract_calls == []
    assert ffmpeg.convert_calls == [
        {
            "input_path": input_file,
            "output_path": expected_audio_path,
            "sample_rate": 16000,
            "channels": 1,
        }
    ]
    assert result.metadata == {
        "job_id": "job-audio-convert",
        "input_path": str(input_file),
        "audio_path": str(expected_audio_path),
        "media_type": "audio",
    }


def test_extract_audio_stage_raises_when_input_path_missing() -> None:
    ffmpeg = FakeFFmpeg()
    context = JobContext(
        job_id="job-missing-input",
        request={"job_id": "job-missing-input"},
        input_uri="input.mp4",
        input_path=None,
        metadata={"media_probe": {"streams": [{"codec_type": "audio"}]}},
    )

    stage = ExtractAudioStage(ffmpeg=ffmpeg)

    with pytest.raises(ValueError, match="context.input_path is required"):
        stage.run(context)

    assert ffmpeg.extract_calls == []
    assert ffmpeg.convert_calls == []


def test_extract_audio_stage_raises_when_probe_missing(tmp_path: Path) -> None:
    input_file = tmp_path / "input.mp4"
    input_file.write_bytes(b"data")

    ffmpeg = FakeFFmpeg()
    context = JobContext(
        job_id="job-missing-probe",
        request={"job_id": "job-missing-probe", "input_uri": str(input_file)},
        input_uri=str(input_file),
        input_path=input_file,
        workspace_dir=tmp_path / "workspace",
    )

    stage = ExtractAudioStage(ffmpeg=ffmpeg)

    with pytest.raises(
        ValueError,
        match=r"context\.metadata\['media_probe'\] is required",
    ):
        stage.run(context)

    assert ffmpeg.extract_calls == []
    assert ffmpeg.convert_calls == []


def test_extract_audio_stage_raises_for_video_without_audio(tmp_path: Path) -> None:
    input_file = tmp_path / "silent.mp4"
    input_file.write_bytes(b"video-data")

    ffmpeg = FakeFFmpeg()
    context = JobContext(
        job_id="job-silent-video",
        request={"job_id": "job-silent-video", "input_uri": str(input_file)},
        workspace_dir=tmp_path / "workspace",
        input_uri=str(input_file),
        input_path=input_file,
        metadata={
            "media_probe": {
                "streams": [
                    {"codec_type": "video", "codec_name": "h264"},
                ]
            }
        },
    )

    stage = ExtractAudioStage(ffmpeg=ffmpeg)

    with pytest.raises(
        ValueError,
        match="Input media contains video streams but no audio streams",
    ):
        stage.run(context)

    assert ffmpeg.extract_calls == []
    assert ffmpeg.convert_calls == []


def test_extract_audio_stage_raises_for_media_with_no_usable_streams(
    tmp_path: Path,
) -> None:
    input_file = tmp_path / "unknown.bin"
    input_file.write_bytes(b"data")

    ffmpeg = FakeFFmpeg()
    context = JobContext(
        job_id="job-no-streams",
        request={"job_id": "job-no-streams", "input_uri": str(input_file)},
        workspace_dir=tmp_path / "workspace",
        input_uri=str(input_file),
        input_path=input_file,
        metadata={"media_probe": {"streams": []}},
    )

    stage = ExtractAudioStage(ffmpeg=ffmpeg)

    with pytest.raises(
        ValueError,
        match="Input media contains no usable audio or video streams",
    ):
        stage.run(context)

    assert ffmpeg.extract_calls == []
    assert ffmpeg.convert_calls == []


def test_extract_audio_stage_uses_workspace_audio_dir_when_job_not_present(
    tmp_path: Path,
) -> None:
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir(parents=True)

    input_file = tmp_path / "voice.mp3"
    input_file.write_bytes(b"audio-data")

    ffmpeg = FakeFFmpeg()
    context = JobContext(
        job_id="job-workspace-fallback",
        request={"job_id": "job-workspace-fallback", "input_uri": str(input_file)},
        workspace_dir=workspace_dir,
        input_uri=str(input_file),
        input_path=input_file,
        metadata={
            "media_probe": {
                "streams": [{"codec_type": "audio", "codec_name": "mp3"}]
            }
        },
    )

    stage = ExtractAudioStage(ffmpeg=ffmpeg, sample_rate=8000, channels=2)

    result = stage.run(context)

    expected_audio_path = workspace_dir / "audio" / "voice.wav"

    assert result.status == StageStatus.SUCCESS
    assert context.audio_path == expected_audio_path
    assert expected_audio_path.exists()
    assert ffmpeg.extract_calls == []
    assert ffmpeg.convert_calls == [
        {
            "input_path": input_file,
            "output_path": expected_audio_path,
            "sample_rate": 8000,
            "channels": 2,
        }
    ]


def test_extract_audio_stage_raises_when_workspace_missing_and_no_job_metadata(
    tmp_path: Path,
) -> None:
    input_file = tmp_path / "voice.mp3"
    input_file.write_bytes(b"audio-data")

    ffmpeg = FakeFFmpeg()
    context = JobContext(
        job_id="job-no-workspace",
        request={"job_id": "job-no-workspace", "input_uri": str(input_file)},
        workspace_dir=None,
        input_uri=str(input_file),
        input_path=input_file,
        metadata={
            "media_probe": {
                "streams": [{"codec_type": "audio", "codec_name": "mp3"}]
            }
        },
    )

    stage = ExtractAudioStage(ffmpeg=ffmpeg)

    with pytest.raises(
        ValueError,
        match="context.workspace_dir is required when no Job is present in metadata",
    ):
        stage.run(context)

    assert ffmpeg.extract_calls == []
    assert ffmpeg.convert_calls == []
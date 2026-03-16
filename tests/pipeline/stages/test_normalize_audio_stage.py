# tests/core/pipeline/stages/test_normalize_audio_stage.py

from __future__ import annotations

from pathlib import Path

import pytest

from core.job.job import Job
from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages.normalize_audio_stage import NormalizeAudioStage


class FakeFFmpeg:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.convert_calls: list[dict[str, object]] = []

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
        output.write_bytes(b"normalized-wav")
        return output


def test_normalize_audio_stage_skips_when_audio_already_normalized(
    tmp_path: Path,
) -> None:
    audio_file = tmp_path / "voice.wav"
    audio_file.write_bytes(b"wav-data")

    ffmpeg = FakeFFmpeg()

    probe = {
        "streams": [
            {
                "codec_type": "audio",
                "codec_name": "pcm_s16le",
                "sample_rate": "16000",
                "channels": 1,
            }
        ]
    }

    context = JobContext(
        job_id="job-skip",
        request={"job_id": "job-skip"},
        workspace_dir=tmp_path / "workspace",
        audio_path=audio_file,
        metadata={"media_probe": probe},
    )

    stage = NormalizeAudioStage(ffmpeg=ffmpeg)

    result = stage.run(context)

    assert result.status == StageStatus.SKIPPED
    assert result.context is context
    assert context.audio_path == audio_file
    assert ffmpeg.convert_calls == []
    assert context.metadata["normalized_audio_probe"] == probe
    assert result.metadata == {
        "job_id": "job-skip",
        "audio_path": str(audio_file),
        "normalized_audio_path": str(audio_file),
        "sample_rate": 16000,
        "channels": 1,
        "codec_name": "pcm_s16le",
    }


def test_normalize_audio_stage_converts_non_wav_audio(tmp_path: Path) -> None:
    audio_file = tmp_path / "voice.mp3"
    audio_file.write_bytes(b"mp3-data")

    job = Job(job_id="job-convert-mp3", base_dir=str(tmp_path / "jobs"))
    ffmpeg = FakeFFmpeg()

    context = JobContext(
        job_id="job-convert-mp3",
        request={"job_id": "job-convert-mp3"},
        workspace_dir=Path(job.root),
        audio_path=audio_file,
        metadata={
            "job": job,
            "media_probe": {
                "streams": [
                    {
                        "codec_type": "audio",
                        "codec_name": "mp3",
                        "sample_rate": "44100",
                        "channels": 2,
                    }
                ]
            },
        },
    )

    stage = NormalizeAudioStage(ffmpeg=ffmpeg)

    result = stage.run(context)

    expected_output = Path(job.audio_dir) / "voice.wav"

    assert result.status == StageStatus.SUCCESS
    assert context.audio_path == expected_output
    assert expected_output.exists()
    assert expected_output.read_bytes() == b"normalized-wav"
    assert ffmpeg.convert_calls == [
        {
            "input_path": audio_file,
            "output_path": expected_output,
            "sample_rate": 16000,
            "channels": 1,
        }
    ]
    assert context.metadata["normalized_audio_probe"] == {
        "streams": [
            {
                "codec_type": "audio",
                "codec_name": "pcm_s16le",
                "sample_rate": "16000",
                "channels": 1,
            }
        ]
    }
    assert result.metadata == {
        "job_id": "job-convert-mp3",
        "audio_path": str(audio_file),
        "normalized_audio_path": str(expected_output),
        "sample_rate": 16000,
        "channels": 1,
        "codec_name": "pcm_s16le",
    }


def test_normalize_audio_stage_converts_wav_when_sample_rate_or_channels_do_not_match(
    tmp_path: Path,
) -> None:
    audio_file = tmp_path / "voice.wav"
    audio_file.write_bytes(b"bad-wav")

    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir(parents=True)

    ffmpeg = FakeFFmpeg()

    context = JobContext(
        job_id="job-convert-wav",
        request={"job_id": "job-convert-wav"},
        workspace_dir=workspace_dir,
        audio_path=audio_file,
        metadata={
            "media_probe": {
                "streams": [
                    {
                        "codec_type": "audio",
                        "codec_name": "pcm_s16le",
                        "sample_rate": "44100",
                        "channels": 2,
                    }
                ]
            }
        },
    )

    stage = NormalizeAudioStage(ffmpeg=ffmpeg)

    result = stage.run(context)

    expected_output = workspace_dir / "audio" / "voice_normalized.wav"

    assert result.status == StageStatus.SUCCESS
    assert context.audio_path == expected_output
    assert expected_output.exists()
    assert ffmpeg.convert_calls == [
        {
            "input_path": audio_file,
            "output_path": expected_output,
            "sample_rate": 16000,
            "channels": 1,
        }
    ]
    assert context.metadata["normalized_audio_probe"] == {
        "streams": [
            {
                "codec_type": "audio",
                "codec_name": "pcm_s16le",
                "sample_rate": "16000",
                "channels": 1,
            }
        ]
    }
    assert result.metadata == {
        "job_id": "job-convert-wav",
        "audio_path": str(audio_file),
        "normalized_audio_path": str(expected_output),
        "sample_rate": 16000,
        "channels": 1,
        "codec_name": "pcm_s16le",
    }


def test_normalize_audio_stage_raises_when_audio_path_missing() -> None:
    ffmpeg = FakeFFmpeg()

    context = JobContext(
        job_id="job-missing-audio",
        request={"job_id": "job-missing-audio"},
        audio_path=None,
        metadata={
            "media_probe": {
                "streams": [
                    {
                        "codec_type": "audio",
                        "codec_name": "pcm_s16le",
                        "sample_rate": "16000",
                        "channels": 1,
                    }
                ]
            }
        },
    )

    stage = NormalizeAudioStage(ffmpeg=ffmpeg)

    with pytest.raises(ValueError, match="context.audio_path is required"):
        stage.run(context)

    assert ffmpeg.convert_calls == []


def test_normalize_audio_stage_raises_when_audio_probe_missing(
    tmp_path: Path,
) -> None:
    audio_file = tmp_path / "input.wav"
    audio_file.write_bytes(b"data")

    ffmpeg = FakeFFmpeg()
    context = JobContext(
        job_id="job-missing-probe",
        request={"job_id": "job-missing-probe"},
        workspace_dir=tmp_path / "workspace",
        audio_path=audio_file,
        metadata={},
    )

    stage = NormalizeAudioStage(ffmpeg=ffmpeg)

    with pytest.raises(
        ValueError,
        match=r"context\.metadata\['media_probe'\] is required",
    ):
        stage.run(context)

    assert ffmpeg.convert_calls == []


def test_normalize_audio_stage_raises_when_probe_has_no_audio_stream(
    tmp_path: Path,
) -> None:
    audio_file = tmp_path / "voice.wav"
    audio_file.write_bytes(b"wav-data")

    ffmpeg = FakeFFmpeg()

    context = JobContext(
        job_id="job-no-audio-stream",
        request={"job_id": "job-no-audio-stream"},
        workspace_dir=tmp_path / "workspace",
        audio_path=audio_file,
        metadata={
            "media_probe": {
                "streams": [
                    {"codec_type": "video", "codec_name": "h264"},
                ]
            }
        },
    )

    stage = NormalizeAudioStage(ffmpeg=ffmpeg)

    with pytest.raises(ValueError, match="Audio probe does not contain an audio stream"):
        stage.run(context)

    assert ffmpeg.convert_calls == []


def test_normalize_audio_stage_uses_workspace_audio_dir_when_job_not_present(
    tmp_path: Path,
) -> None:
    audio_file = tmp_path / "speech.m4a"
    audio_file.write_bytes(b"m4a-data")

    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir(parents=True)

    ffmpeg = FakeFFmpeg()

    context = JobContext(
        job_id="job-workspace-fallback",
        request={"job_id": "job-workspace-fallback"},
        workspace_dir=workspace_dir,
        audio_path=audio_file,
        metadata={
            "media_probe": {
                "streams": [
                    {
                        "codec_type": "audio",
                        "codec_name": "aac",
                        "sample_rate": "48000",
                        "channels": 2,
                    }
                ]
            }
        },
    )

    stage = NormalizeAudioStage(ffmpeg=ffmpeg, sample_rate=8000, channels=2)

    result = stage.run(context)

    expected_output = workspace_dir / "audio" / "speech.wav"

    assert result.status == StageStatus.SUCCESS
    assert context.audio_path == expected_output
    assert expected_output.exists()
    assert ffmpeg.convert_calls == [
        {
            "input_path": audio_file,
            "output_path": expected_output,
            "sample_rate": 8000,
            "channels": 2,
        }
    ]
    assert context.metadata["normalized_audio_probe"] == {
        "streams": [
            {
                "codec_type": "audio",
                "codec_name": "pcm_s16le",
                "sample_rate": "8000",
                "channels": 2,
            }
        ]
    }


def test_normalize_audio_stage_raises_when_workspace_missing_and_no_job_metadata(
    tmp_path: Path,
) -> None:
    audio_file = tmp_path / "speech.m4a"
    audio_file.write_bytes(b"m4a-data")

    ffmpeg = FakeFFmpeg()

    context = JobContext(
        job_id="job-no-workspace",
        request={"job_id": "job-no-workspace"},
        workspace_dir=None,
        audio_path=audio_file,
        metadata={
            "media_probe": {
                "streams": [
                    {
                        "codec_type": "audio",
                        "codec_name": "aac",
                        "sample_rate": "48000",
                        "channels": 2,
                    }
                ]
            }
        },
    )

    stage = NormalizeAudioStage(ffmpeg=ffmpeg)

    with pytest.raises(
        ValueError,
        match="context.workspace_dir is required when no Job is present in metadata",
    ):
        stage.run(context)

    assert ffmpeg.convert_calls == []
# tests/pipeline/stages/test_diarize_audio_stage.py

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages.diarize_audio_stage import DiarizeAudioStage


def test_diarize_audio_stage_diarizes_audio_and_updates_context_metadata(tmp_path):
    audio_path = tmp_path / "normalized.wav"
    audio_path.write_bytes(b"fake-wav")

    diarization = Mock(name="diarization_result")
    diarizer = Mock()
    diarizer.diarize.return_value = diarization

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"normalized_audio_path": str(audio_path)},
    )

    stage = DiarizeAudioStage(diarizer=diarizer)

    result = stage.run(context)

    assert result.status == StageStatus.SUCCESS
    assert result.context is context
    assert context.metadata["diarization"] is diarization
    assert result.metadata == {"diarization": diarization}
    diarizer.diarize.assert_called_once_with(audio_path)


def test_diarize_audio_stage_raises_when_normalized_audio_path_missing():
    diarizer = Mock()

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={},
    )

    stage = DiarizeAudioStage(diarizer=diarizer)

    with pytest.raises(
        ValueError,
        match=r"context\.metadata\['normalized_audio_path'\] is required for DiarizeAudioStage",
    ):
        stage.run(context)

    diarizer.diarize.assert_not_called()


def test_diarize_audio_stage_skips_when_diarization_already_exists_in_metadata(tmp_path):
    audio_path = tmp_path / "normalized.wav"
    audio_path.write_bytes(b"fake-wav")

    existing_diarization = Mock(name="existing_diarization")
    diarizer = Mock()

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={
            "normalized_audio_path": str(audio_path),
            "diarization": existing_diarization,
        },
    )

    stage = DiarizeAudioStage(diarizer=diarizer)

    result = stage.run(context)

    assert result.status == StageStatus.SKIPPED
    assert result.context is context
    assert context.metadata["diarization"] is existing_diarization
    assert result.metadata == {"diarization": existing_diarization}
    diarizer.diarize.assert_not_called()


def test_diarize_audio_stage_passes_path_object_to_diarizer(tmp_path):
    audio_path = tmp_path / "normalized.wav"
    audio_path.write_bytes(b"fake-wav")

    diarization = Mock(name="diarization_result")
    diarizer = Mock()
    diarizer.diarize.return_value = diarization

    context = JobContext(
        job_id="job-456",
        request={"input": "file.mp4"},
        metadata={"normalized_audio_path": str(audio_path)},
    )

    stage = DiarizeAudioStage(diarizer=diarizer)
    stage.run(context)

    called_arg = diarizer.diarize.call_args.args[0]
    assert isinstance(called_arg, Path)
    assert called_arg == audio_path
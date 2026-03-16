# tests/pipeline/stages/test_transcribe_audio_stage.py

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages.transcribe_audio_stage import TranscribeAudioStage


def test_transcribe_audio_stage_transcribes_audio_and_updates_context_metadata(tmp_path):
    audio_path = tmp_path / "normalized.wav"
    audio_path.write_bytes(b"fake-wav")

    transcript = Mock(name="transcript_result")
    transcriber = Mock()
    transcriber.transcribe.return_value = transcript

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"normalized_audio_path": str(audio_path)},
    )

    stage = TranscribeAudioStage(transcriber=transcriber)

    result = stage.run(context)

    assert result.status == StageStatus.SUCCESS
    assert result.context is context
    assert context.metadata["transcript"] is transcript
    assert result.metadata == {"transcript": transcript}
    transcriber.transcribe.assert_called_once_with(audio_path)


def test_transcribe_audio_stage_raises_when_normalized_audio_path_missing():
    transcriber = Mock()

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={},
    )

    stage = TranscribeAudioStage(transcriber=transcriber)

    with pytest.raises(
        ValueError,
        match=r"context\.metadata\['normalized_audio_path'\] is required for TranscribeAudioStage",
    ):
        stage.run(context)

    transcriber.transcribe.assert_not_called()


def test_transcribe_audio_stage_skips_when_transcript_already_exists_in_metadata(tmp_path):
    audio_path = tmp_path / "normalized.wav"
    audio_path.write_bytes(b"fake-wav")

    existing_transcript = Mock(name="existing_transcript")
    transcriber = Mock()

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={
            "normalized_audio_path": str(audio_path),
            "transcript": existing_transcript,
        },
    )

    stage = TranscribeAudioStage(transcriber=transcriber)

    result = stage.run(context)

    assert result.status == StageStatus.SKIPPED
    assert result.context is context
    assert context.metadata["transcript"] is existing_transcript
    assert result.metadata == {"transcript": existing_transcript}
    transcriber.transcribe.assert_not_called()


def test_transcribe_audio_stage_passes_path_object_to_transcriber(tmp_path):
    audio_path = tmp_path / "normalized.wav"
    audio_path.write_bytes(b"fake-wav")

    transcript = Mock(name="transcript_result")
    transcriber = Mock()
    transcriber.transcribe.return_value = transcript

    context = JobContext(
        job_id="job-456",
        request={"input": "file.mp4"},
        metadata={"normalized_audio_path": str(audio_path)},
    )

    stage = TranscribeAudioStage(transcriber=transcriber)
    stage.run(context)

    called_arg = transcriber.transcribe.call_args.args[0]
    assert isinstance(called_arg, Path)
    assert called_arg == audio_path
# tests/pipeline/stages/test_align_transcript_stage.py

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages.align_transcript_stage import AlignTranscriptStage


@dataclass(slots=True)
class FakeTranscriptSegment:
    start: float
    end: float
    text: str
    speaker: str | None = None
    words: list[dict] = field(default_factory=list)


@dataclass(slots=True)
class FakeTranscriptResult:
    text: str
    language: str | None = None
    language_probability: float | None = None
    segments: list[FakeTranscriptSegment] = field(default_factory=list)
    raw: dict = field(default_factory=dict)


@dataclass(slots=True)
class FakeSpeakerSegment:
    start: float
    end: float
    speaker: str


@dataclass(slots=True)
class FakeDiarizationResult:
    segments: list[FakeSpeakerSegment] = field(default_factory=list)
    annotation: object | None = None
    raw: dict = field(default_factory=dict)


def test_align_transcript_stage_aligns_transcript_segments_with_speakers():
    transcript = FakeTranscriptResult(
        text="hello there general kenobi",
        segments=[
            FakeTranscriptSegment(start=0.0, end=1.0, text="hello there"),
            FakeTranscriptSegment(start=1.0, end=2.0, text="general kenobi"),
        ],
    )
    diarization = FakeDiarizationResult(
        segments=[
            FakeSpeakerSegment(start=0.1, end=1.2, speaker="SPEAKER_00"),
            FakeSpeakerSegment(start=1.2, end=2.2, speaker="SPEAKER_01"),
        ]
    )

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={
            "transcript": transcript,
            "diarization": diarization,
        },
    )

    stage = AlignTranscriptStage()

    result = stage.run(context)

    aligned_transcript = context.metadata["aligned_transcript"]

    assert result.status == StageStatus.SUCCESS
    assert result.context is context
    assert result.metadata == {"aligned_transcript": aligned_transcript}
    assert aligned_transcript is not transcript
    assert aligned_transcript.text == transcript.text
    assert len(aligned_transcript.segments) == 2
    assert aligned_transcript.segments[0].speaker == "SPEAKER_00"
    assert aligned_transcript.segments[1].speaker == "SPEAKER_01"

    # original transcript remains unchanged
    assert transcript.segments[0].speaker is None
    assert transcript.segments[1].speaker is None


def test_align_transcript_stage_uses_max_overlap_not_start_containment_only():
    transcript = FakeTranscriptResult(
        text="first second",
        segments=[
            FakeTranscriptSegment(start=0.0, end=1.28, text="first"),
            FakeTranscriptSegment(start=3.36, end=4.68, text="second"),
        ],
    )
    diarization = FakeDiarizationResult(
        segments=[
            FakeSpeakerSegment(start=0.03, end=2.35, speaker="SPEAKER_02"),
            FakeSpeakerSegment(start=3.42, end=5.43, speaker="SPEAKER_01"),
        ]
    )

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={
            "transcript": transcript,
            "diarization": diarization,
        },
    )

    stage = AlignTranscriptStage()

    result = stage.run(context)

    aligned_transcript = result.metadata["aligned_transcript"]

    assert result.status == StageStatus.SUCCESS
    assert aligned_transcript.segments[0].speaker == "SPEAKER_02"
    assert aligned_transcript.segments[1].speaker == "SPEAKER_01"


def test_align_transcript_stage_sets_unknown_when_no_overlap_exists():
    transcript = FakeTranscriptResult(
        text="lonely segment",
        segments=[
            FakeTranscriptSegment(start=10.0, end=11.0, text="lonely segment"),
        ],
    )
    diarization = FakeDiarizationResult(
        segments=[
            FakeSpeakerSegment(start=0.0, end=1.0, speaker="SPEAKER_00"),
        ]
    )

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={
            "transcript": transcript,
            "diarization": diarization,
        },
    )

    stage = AlignTranscriptStage()

    result = stage.run(context)

    aligned_transcript = result.metadata["aligned_transcript"]

    assert result.status == StageStatus.SUCCESS
    assert aligned_transcript.segments[0].speaker == "UNKNOWN"


def test_align_transcript_stage_raises_when_transcript_missing():
    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"diarization": FakeDiarizationResult()},
    )

    stage = AlignTranscriptStage()

    with pytest.raises(
        ValueError,
        match=r"context\.metadata\['transcript'\] is required for AlignTranscriptStage",
    ):
        stage.run(context)


def test_align_transcript_stage_raises_when_diarization_missing():
    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"transcript": FakeTranscriptResult(text="hello")},
    )

    stage = AlignTranscriptStage()

    with pytest.raises(
        ValueError,
        match=r"context\.metadata\['diarization'\] is required for AlignTranscriptStage",
    ):
        stage.run(context)


def test_align_transcript_stage_skips_when_aligned_transcript_already_exists():
    transcript = FakeTranscriptResult(text="hello")
    diarization = FakeDiarizationResult()
    existing_aligned_transcript = FakeTranscriptResult(text="already aligned")

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={
            "transcript": transcript,
            "diarization": diarization,
            "aligned_transcript": existing_aligned_transcript,
        },
    )

    stage = AlignTranscriptStage()

    result = stage.run(context)

    assert result.status == StageStatus.SKIPPED
    assert result.context is context
    assert result.metadata == {"aligned_transcript": existing_aligned_transcript}
    assert context.metadata["aligned_transcript"] is existing_aligned_transcript
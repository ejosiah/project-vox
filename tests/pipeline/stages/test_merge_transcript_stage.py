# tests/pipeline/stages/test_merge_transcript_stage.py

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages.merge_transcript_stage import MergeTranscriptStage


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


def test_merge_transcript_stage_merges_adjacent_segments_with_same_speaker():
    aligned_transcript = FakeTranscriptResult(
        text="hello there general kenobi",
        segments=[
            FakeTranscriptSegment(
                start=0.0,
                end=1.0,
                text="hello there",
                speaker="SPEAKER_00",
                words=[{"word": "hello"}],
            ),
            FakeTranscriptSegment(
                start=1.0,
                end=2.0,
                text="general kenobi",
                speaker="SPEAKER_00",
                words=[{"word": "general"}],
            ),
            FakeTranscriptSegment(
                start=2.0,
                end=3.0,
                text="you are a bold one",
                speaker="SPEAKER_01",
                words=[{"word": "you"}],
            ),
        ],
    )

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"aligned_transcript": aligned_transcript},
    )

    stage = MergeTranscriptStage()

    result = stage.run(context)

    merged_transcript = context.metadata["merged_transcript"]

    assert result.status == StageStatus.SUCCESS
    assert result.context is context
    assert result.metadata == {"merged_transcript": merged_transcript}

    assert merged_transcript is not aligned_transcript
    assert merged_transcript.text == "hello there general kenobi you are a bold one"
    assert len(merged_transcript.segments) == 2

    assert merged_transcript.segments[0].start == 0.0
    assert merged_transcript.segments[0].end == 2.0
    assert merged_transcript.segments[0].text == "hello there general kenobi"
    assert merged_transcript.segments[0].speaker == "SPEAKER_00"
    assert merged_transcript.segments[0].words == [
        {"word": "hello"},
        {"word": "general"},
    ]

    assert merged_transcript.segments[1].start == 2.0
    assert merged_transcript.segments[1].end == 3.0
    assert merged_transcript.segments[1].text == "you are a bold one"
    assert merged_transcript.segments[1].speaker == "SPEAKER_01"

    # original transcript remains unchanged
    assert len(aligned_transcript.segments) == 3
    assert aligned_transcript.segments[0].end == 1.0
    assert aligned_transcript.segments[0].text == "hello there"


def test_merge_transcript_stage_does_not_merge_non_adjacent_same_speaker_runs():
    aligned_transcript = FakeTranscriptResult(
        text="a b c",
        segments=[
            FakeTranscriptSegment(start=0.0, end=1.0, text="a", speaker="SPEAKER_00"),
            FakeTranscriptSegment(start=1.0, end=2.0, text="b", speaker="SPEAKER_01"),
            FakeTranscriptSegment(start=2.0, end=3.0, text="c", speaker="SPEAKER_00"),
        ],
    )

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"aligned_transcript": aligned_transcript},
    )

    stage = MergeTranscriptStage()

    result = stage.run(context)

    merged_transcript = result.metadata["merged_transcript"]

    assert result.status == StageStatus.SUCCESS
    assert len(merged_transcript.segments) == 3
    assert [segment.speaker for segment in merged_transcript.segments] == [
        "SPEAKER_00",
        "SPEAKER_01",
        "SPEAKER_00",
    ]


def test_merge_transcript_stage_merges_unknown_segments_too():
    aligned_transcript = FakeTranscriptResult(
        text="one two three",
        segments=[
            FakeTranscriptSegment(start=0.0, end=1.0, text="one", speaker="UNKNOWN"),
            FakeTranscriptSegment(start=1.0, end=2.0, text="two", speaker="UNKNOWN"),
            FakeTranscriptSegment(start=2.0, end=3.0, text="three", speaker="SPEAKER_01"),
        ],
    )

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"aligned_transcript": aligned_transcript},
    )

    stage = MergeTranscriptStage()

    result = stage.run(context)

    merged_transcript = result.metadata["merged_transcript"]

    assert result.status == StageStatus.SUCCESS
    assert len(merged_transcript.segments) == 2
    assert merged_transcript.segments[0].speaker == "UNKNOWN"
    assert merged_transcript.segments[0].text == "one two"
    assert merged_transcript.segments[0].end == 2.0


def test_merge_transcript_stage_handles_empty_segments():
    aligned_transcript = FakeTranscriptResult(
        text="",
        segments=[],
    )

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"aligned_transcript": aligned_transcript},
    )

    stage = MergeTranscriptStage()

    result = stage.run(context)

    merged_transcript = result.metadata["merged_transcript"]

    assert result.status == StageStatus.SUCCESS
    assert merged_transcript.segments == []
    assert merged_transcript.text == ""


def test_merge_transcript_stage_raises_when_aligned_transcript_missing():
    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={},
    )

    stage = MergeTranscriptStage()

    with pytest.raises(
        ValueError,
        match=r"context\.metadata\['aligned_transcript'\] is required for MergeTranscriptStage",
    ):
        stage.run(context)


def test_merge_transcript_stage_skips_when_merged_transcript_already_exists():
    aligned_transcript = FakeTranscriptResult(text="hello")
    existing_merged_transcript = FakeTranscriptResult(text="already merged")

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={
            "aligned_transcript": aligned_transcript,
            "merged_transcript": existing_merged_transcript,
        },
    )

    stage = MergeTranscriptStage()

    result = stage.run(context)

    assert result.status == StageStatus.SKIPPED
    assert result.context is context
    assert result.metadata == {"merged_transcript": existing_merged_transcript}
    assert context.metadata["merged_transcript"] is existing_merged_transcript
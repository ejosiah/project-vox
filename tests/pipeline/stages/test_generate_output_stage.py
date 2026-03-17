# tests/pipeline/stages/test_generate_output_stage.py

from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest

from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages.generate_output_stage import GenerateOutputStage


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

    def to_dict(self) -> dict:
        return {
            "text": self.text,
            "language": self.language,
            "language_probability": self.language_probability,
            "segments": [
                {
                    "start": segment.start,
                    "end": segment.end,
                    "text": segment.text,
                    "speaker": segment.speaker,
                    "words": segment.words,
                }
                for segment in self.segments
            ],
            "raw": self.raw,
        }


def _make_context(tmp_path, transcript, output_types):
    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={
            "workspace_dir": str(tmp_path),
            "merged_transcript": transcript,
        },
    )
    context.output_types = output_types
    return context


def test_generate_output_stage_generates_requested_outputs(tmp_path):
    transcript = FakeTranscriptResult(
        text="hello world again",
        segments=[
            FakeTranscriptSegment(
                start=0.0,
                end=1.25,
                text="hello world",
                speaker="SPEAKER_00",
            ),
            FakeTranscriptSegment(
                start=1.25,
                end=2.5,
                text="again",
                speaker="SPEAKER_01",
            ),
        ],
    )
    context = _make_context(tmp_path, transcript, ["txt", "json", "srt", "vtt"])

    stage = GenerateOutputStage()
    result = stage.run(context)

    assert result.status == StageStatus.SUCCESS
    assert result.context is context

    generated_outputs = result.metadata["generated_outputs"]
    assert set(generated_outputs.keys()) == {"txt", "json", "srt", "vtt"}
    assert context.metadata["generated_outputs"] == generated_outputs

    txt_path = tmp_path / "output" / "transcript.txt"
    json_path = tmp_path / "output" / "transcript.json"
    srt_path = tmp_path / "output" / "transcript.srt"
    vtt_path = tmp_path / "output" / "transcript.vtt"

    assert generated_outputs["txt"] == str(txt_path)
    assert generated_outputs["json"] == str(json_path)
    assert generated_outputs["srt"] == str(srt_path)
    assert generated_outputs["vtt"] == str(vtt_path)

    assert txt_path.read_text(encoding="utf-8") == (
        "SPEAKER_00: hello world\n"
        "SPEAKER_01: again\n"
    )

    json_payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert json_payload["text"] == "hello world again"
    assert json_payload["segments"][0]["speaker"] == "SPEAKER_00"
    assert json_payload["segments"][1]["text"] == "again"

    assert srt_path.read_text(encoding="utf-8") == (
        "1\n"
        "00:00:00,000 --> 00:00:01,250\n"
        "SPEAKER_00: hello world\n\n"
        "2\n"
        "00:00:01,250 --> 00:00:02,500\n"
        "SPEAKER_01: again\n"
    )

    assert vtt_path.read_text(encoding="utf-8") == (
        "WEBVTT\n\n"
        "00:00:00.000 --> 00:00:01.250\n"
        "SPEAKER_00: hello world\n\n"
        "00:00:01.250 --> 00:00:02.500\n"
        "SPEAKER_01: again\n"
    )


def test_generate_output_stage_generates_only_requested_output_types(tmp_path):
    transcript = FakeTranscriptResult(
        text="hello world",
        segments=[
            FakeTranscriptSegment(
                start=0.0,
                end=1.0,
                text="hello world",
                speaker="SPEAKER_00",
            )
        ],
    )
    context = _make_context(tmp_path, transcript, ["txt", "json"])

    stage = GenerateOutputStage()
    result = stage.run(context)

    generated_outputs = result.metadata["generated_outputs"]

    assert result.status == StageStatus.SUCCESS
    assert set(generated_outputs.keys()) == {"txt", "json"}
    assert (tmp_path / "output" / "transcript.txt").exists()
    assert (tmp_path / "output" / "transcript.json").exists()
    assert not (tmp_path / "output" / "transcript.srt").exists()
    assert not (tmp_path / "output" / "transcript.vtt").exists()


def test_generate_output_stage_uses_unknown_when_speaker_missing(tmp_path):
    transcript = FakeTranscriptResult(
        text="hello",
        segments=[
            FakeTranscriptSegment(
                start=0.0,
                end=1.0,
                text="hello",
                speaker=None,
            )
        ],
    )
    context = _make_context(tmp_path, transcript, ["txt"])

    stage = GenerateOutputStage()
    stage.run(context)

    txt_path = tmp_path / "output" / "transcript.txt"
    assert txt_path.read_text(encoding="utf-8") == "UNKNOWN: hello\n"


def test_generate_output_stage_raises_when_output_types_missing(tmp_path):
    transcript = FakeTranscriptResult(text="hello")
    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={
            "workspace_dir": str(tmp_path),
            "merged_transcript": transcript,
        },
    )

    stage = GenerateOutputStage()

    with pytest.raises(
        ValueError,
        match=r"context\.output_types is required for GenerateOutputStage",
    ):
        stage.run(context)


def test_generate_output_stage_raises_when_workspace_dir_missing():
    transcript = FakeTranscriptResult(text="hello")
    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"merged_transcript": transcript},
    )
    context.output_types = ["txt"]

    stage = GenerateOutputStage()

    with pytest.raises(
        ValueError,
        match=r"context\.metadata\['workspace_dir'\] is required for GenerateOutputStage",
    ):
        stage.run(context)


def test_generate_output_stage_raises_when_merged_transcript_missing(tmp_path):
    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={"workspace_dir": str(tmp_path)},
    )
    context.output_types = ["txt"]

    stage = GenerateOutputStage()

    with pytest.raises(
        ValueError,
        match=r"context\.metadata\['merged_transcript'\] is required for GenerateOutputStage",
    ):
        stage.run(context)


def test_generate_output_stage_raises_when_output_type_is_unsupported(tmp_path):
    transcript = FakeTranscriptResult(text="hello")
    context = _make_context(tmp_path, transcript, ["txt", "pdf"])

    stage = GenerateOutputStage()

    with pytest.raises(
        ValueError,
        match=r"Unsupported output types requested: pdf",
    ):
        stage.run(context)


def test_generate_output_stage_skips_when_generated_outputs_already_exist(tmp_path):
    transcript = FakeTranscriptResult(text="hello")
    existing_outputs = {"txt": str(tmp_path / "output" / "transcript.txt")}

    context = JobContext(
        job_id="job-123",
        request={"input": "file.mp4"},
        metadata={
            "workspace_dir": str(tmp_path),
            "merged_transcript": transcript,
            "generated_outputs": existing_outputs,
        },
    )
    context.output_types = ["txt"]

    stage = GenerateOutputStage()
    result = stage.run(context)

    assert result.status == StageStatus.SKIPPED
    assert result.context is context
    assert result.metadata == {"generated_outputs": existing_outputs}
    assert context.metadata["generated_outputs"] is existing_outputs
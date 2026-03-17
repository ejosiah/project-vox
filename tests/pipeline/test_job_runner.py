# tests/pipeline/test_job_runner.py

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from core.pipeline.job_runner import JobRunner, StageExecutionError
from core.pipeline.stage import Stage, StageResult, StageStatus


@dataclass
class DummyContext:
    job_id: str | None = "job-123"
    events: list[str] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)


class RecordingStage(Stage):
    def __init__(
        self,
        name: str,
        *,
        status: StageStatus = StageStatus.SUCCESS,
        should_run_value: bool = True,
        replacement_context: object | None = None,
        raise_exc: Exception | None = None,
        is_cleanup: bool = False,
    ) -> None:
        self.name = name
        self._status = status
        self._should_run_value = should_run_value
        self._replacement_context = replacement_context
        self._raise_exc = raise_exc
        self.is_cleanup = is_cleanup

    def should_run(self, context: object) -> bool:
        return self._should_run_value

    def run(self, context: object) -> StageResult:
        if self._raise_exc is not None:
            raise self._raise_exc

        if hasattr(context, "events"):
            context.events.append(self.name)
        elif isinstance(context, dict):
            context.setdefault("events", []).append(self.name)

        next_context = self._replacement_context if self._replacement_context is not None else context
        return StageResult(status=self._status, context=next_context)


class CleanupStage(RecordingStage):
    pass


def test_runs_all_non_cleanup_stages_in_order() -> None:
    context = DummyContext()
    runner = JobRunner(
        [
            RecordingStage("validate"),
            RecordingStage("download"),
            RecordingStage("transcribe"),
        ]
    )

    result = runner.run(context)

    assert result is context
    assert result.events == ["validate", "download", "transcribe"]


def test_returns_updated_context_when_stage_replaces_context() -> None:
    original = DummyContext(job_id="job-123")
    replacement = DummyContext(job_id="job-456", events=["replace"])
    runner = JobRunner(
        [
            RecordingStage("validate"),
            RecordingStage("replace", replacement_context=replacement),
        ]
    )

    result = runner.run(original)

    assert result is replacement
    assert result.job_id == "job-456"
    assert result.events == ["replace"]


def test_skips_stage_when_should_run_returns_false() -> None:
    context = DummyContext()
    runner = JobRunner(
        [
            RecordingStage("validate"),
            RecordingStage("download", should_run_value=False),
            RecordingStage("transcribe"),
        ]
    )

    result = runner.run(context)

    assert result.events == ["validate", "transcribe"]


def test_runs_cleanup_stage_after_success() -> None:
    context = DummyContext()
    runner = JobRunner(
        [
            RecordingStage("validate"),
            RecordingStage("transcribe"),
            RecordingStage("cleanup", is_cleanup=True),
        ]
    )

    result = runner.run(context)

    assert result.events == ["validate", "transcribe", "cleanup"]


def test_runs_cleanup_stage_after_failure() -> None:
    context = DummyContext()
    runner = JobRunner(
        [
            RecordingStage("validate"),
            RecordingStage("download", raise_exc=RuntimeError("boom")),
            RecordingStage("cleanup", is_cleanup=True),
        ]
    )

    with pytest.raises(StageExecutionError) as exc_info:
        runner.run(context)

    assert exc_info.value.stage_name == "download"
    assert exc_info.value.job_id == "job-123"
    assert "boom" in str(exc_info.value)
    assert context.events == ["validate", "cleanup"]


def test_stops_executing_non_cleanup_stages_after_failure() -> None:
    context = DummyContext()
    runner = JobRunner(
        [
            RecordingStage("validate"),
            RecordingStage("download", raise_exc=RuntimeError("boom")),
            RecordingStage("transcribe"),
            RecordingStage("cleanup", is_cleanup=True),
        ]
    )

    with pytest.raises(StageExecutionError):
        runner.run(context)

    assert context.events == ["validate", "cleanup"]


def test_cleanup_failure_is_swallowed() -> None:
    context = DummyContext()
    runner = JobRunner(
        [
            RecordingStage("validate"),
            RecordingStage("cleanup", raise_exc=RuntimeError("cleanup boom"), is_cleanup=True),
        ]
    )

    result = runner.run(context)

    assert result is context
    assert result.events == ["validate"]


def test_extracts_job_id_from_dict_context() -> None:
    context = {"job_id": "job-789", "events": [], "metadata" : {}}
    runner = JobRunner(
        [
            RecordingStage("validate"),
            RecordingStage("cleanup", is_cleanup=True),
        ]
    )

    result = runner.run(context)

    assert result["job_id"] == "job-789"
    assert result["events"] == ["validate", "cleanup"]


def test_cleanup_stage_detected_by_name_without_is_cleanup_flag() -> None:
    context = DummyContext()
    runner = JobRunner(
        [
            RecordingStage("validate"),
            RecordingStage("cleanup"),
        ]
    )

    result = runner.run(context)

    assert result.events == ["validate", "cleanup"]


def test_should_run_missing_defaults_to_true() -> None:
    @dataclass
    class NoShouldRunContext:
        job_id: str = "job-123"
        events: list[str] = field(default_factory=list)
        metadata: dict[str, str] = field(default_factory=dict)

    class NoShouldRunStage(Stage):
        name = "validate"

        def run(self, context: NoShouldRunContext) -> StageResult:
            context.events.append(self.name)
            return StageResult(status=StageStatus.SUCCESS, context=context)

    context = NoShouldRunContext()
    runner = JobRunner([NoShouldRunStage()])

    result = runner.run(context)

    assert result.events == ["validate"]


def test_failed_stage_result_raises_error() -> None:
    context = DummyContext()
    runner = JobRunner(
        [
            RecordingStage("validate"),
            RecordingStage("download", status=StageStatus.FAILED),
            RecordingStage("cleanup", is_cleanup=True),
        ]
    )

    with pytest.raises(StageExecutionError) as exc_info:
        runner.run(context)

    assert exc_info.value.stage_name == "download"
    assert exc_info.value.job_id == "job-123"
    assert "FAILED status" in str(exc_info.value)
    assert context.events == ["validate", "download", "cleanup"]


def test_skipped_stage_result_does_not_replace_context() -> None:
    context = DummyContext()
    replacement = DummyContext(job_id="job-999", events=["should-not-be-used"])
    runner = JobRunner(
        [
            RecordingStage("validate"),
            RecordingStage(
                "download",
                status=StageStatus.SKIPPED,
                replacement_context=replacement,
            ),
            RecordingStage("transcribe"),
        ]
    )

    result = runner.run(context)

    assert result is context
    assert result.events == ["validate", "download", "transcribe"]


def test_runner_requires_at_least_one_stage() -> None:
    with pytest.raises(ValueError, match="JobRunner requires at least one stage"):
        JobRunner([])
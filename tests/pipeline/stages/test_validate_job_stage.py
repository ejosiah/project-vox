# tests/pipeline/stages/test_validate_job_stage.py

from __future__ import annotations

from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages import ValidateJobStage


class TestValidateJobStage:
    def test_run_returns_success_for_valid_request(self) -> None:
        context = JobContext.from_kafka_message(
            {
                "job_id": "job-123",
                "input_uri": "s3://bucket/input.mp4",
            }
        )
        stage = ValidateJobStage()

        result = stage.run(context)

        assert result.status == StageStatus.SUCCESS
        assert result.context is context
        assert result.context.error is None
        assert result.context.job_id == "job-123"
        assert result.context.input_uri == "s3://bucket/input.mp4"

    def test_run_returns_failure_when_request_is_missing_required_fields(self) -> None:
        context = JobContext(
            job_id="job-123",
            request={"job_id": "job-123"},
            input_uri=None,
        )
        stage = ValidateJobStage()

        result = stage.run(context)

        assert result.status == StageStatus.FAILED
        assert result.context is context
        assert result.context.error == "Missing required job fields: input_uri"

    def test_run_returns_failure_when_job_id_is_empty(self) -> None:
        context = JobContext(
            job_id="",
            request={
                "job_id": "",
                "input_uri": "s3://bucket/input.mp4",
            },
            input_uri="s3://bucket/input.mp4",
        )
        stage = ValidateJobStage()

        result = stage.run(context)

        assert result.status == StageStatus.FAILED
        assert result.context is context
        assert result.context.error == "Invalid or empty job_id"

    def test_run_returns_failure_when_input_uri_is_empty(self) -> None:
        context = JobContext(
            job_id="job-123",
            request={
                "job_id": "job-123",
                "input_uri": "",
            },
            input_uri=None,
        )
        stage = ValidateJobStage()

        result = stage.run(context)

        assert result.status == StageStatus.FAILED
        assert result.context is context
        assert result.context.error == "input_uri is required"

    def test_run_sets_input_uri_from_request(self) -> None:
        context = JobContext(
            job_id="job-123",
            request={
                "job_id": "job-123",
                "input_uri": "s3://bucket/new-input.mp4",
            },
            input_uri=None,
        )
        stage = ValidateJobStage()

        result = stage.run(context)

        assert result.status == StageStatus.SUCCESS
        assert result.context is context
        assert result.context.input_uri == "s3://bucket/new-input.mp4"
        assert result.context.error is None

    def test_run_respects_custom_required_fields(self) -> None:
        context = JobContext(
            job_id="job-123",
            request={
                "job_id": "job-123",
                "input_uri": "s3://bucket/input.mp4",
            },
            input_uri="s3://bucket/input.mp4",
        )
        stage = ValidateJobStage(required_fields=("job_id", "input_uri", "language"))

        result = stage.run(context)

        assert result.status == StageStatus.FAILED
        assert result.context is context
        assert result.context.error == "Missing required job fields: language"
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
                "source": "s3://bucket/input.mp4",
            }
        )
        stage = ValidateJobStage()

        result = stage.run(context)

        assert result.status == StageStatus.SUCCESS
        assert result.context is context
        assert result.context.error is None
        assert result.context.job_id == "job-123"

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
        assert result.context.error == "Missing required job fields: source"

    def test_run_returns_failure_when_job_id_is_empty(self) -> None:
        context = JobContext(
            job_id="",
            request={
                "job_id": "",
                "source": "s3://bucket/input.mp4",
            },
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
                "source": "",
            },
        )
        stage = ValidateJobStage()

        result = stage.run(context)

        assert result.status == StageStatus.FAILED
        assert result.context is context
        assert result.context.error == "source is required"

    def test_run_sets_input_uri_from_request(self) -> None:
        context = JobContext(
            job_id="job-123",
            request={
                "job_id": "job-123",
                "source": "s3://bucket/new-input.mp4",
            },
        )
        stage = ValidateJobStage()

        result = stage.run(context)

        assert result.status == StageStatus.SUCCESS
        assert result.context is context
        assert result.context.error is None

    def test_run_respects_custom_required_fields(self) -> None:
        context = JobContext(
            job_id="job-123",
            request={
                "job_id": "job-123",
                "source": "s3://bucket/input.mp4",
            },
        )
        stage = ValidateJobStage(required_fields=("job_id", "source", "language"))

        result = stage.run(context)

        assert result.status == StageStatus.FAILED
        assert result.context is context
        assert result.context.error == "Missing required job fields: language"
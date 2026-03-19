# tests/api/mappers/job_response_mapper_test.py

from __future__ import annotations

from datetime import datetime, timezone

from api.mappers.job_response_mapper import build_create_job_response
from api.models.job import JobState


def test_build_create_job_response_from_datetime():
    created_at = datetime(2026, 3, 19, 12, 0, 0, tzinfo=timezone.utc)

    result = build_create_job_response(
        job_id="job_123",
        correlation_id="corr_123",
        created_at=created_at,
        schema_version="v1",
    )

    assert result.job_id == "job_123"
    assert result.correlation_id == "corr_123"
    assert result.state == JobState.QUEUED
    assert result.created_at == "2026-03-19T12:00:00Z"
    assert result.message_type == "job.request"
    assert result.schema_version == "v1"

    assert result.links.self == "/v1/jobs/job_123"
    assert result.links.status == "/v1/jobs/job_123/status"
    assert result.links.result == "/v1/jobs/job_123/result"


def test_build_create_job_response_from_string():
    result = build_create_job_response(
        job_id="job_456",
        correlation_id="corr_456",
        created_at="2026-03-19T12:00:00Z",
        schema_version="v2",
    )

    assert result.job_id == "job_456"
    assert result.correlation_id == "corr_456"
    assert result.state == JobState.QUEUED
    assert result.created_at == "2026-03-19T12:00:00Z"
    assert result.message_type == "job.request"
    assert result.schema_version == "v2"

    assert result.links.self == "/v1/jobs/job_456"
    assert result.links.status == "/v1/jobs/job_456/status"
    assert result.links.result == "/v1/jobs/job_456/result"


def test_build_create_job_response_supports_custom_base_path():
    result = build_create_job_response(
        job_id="job_789",
        correlation_id="corr_789",
        created_at="2026-03-19T12:00:00Z",
        schema_version="v1",
        base_path="/api/jobs/",
    )

    assert result.message_type == "job.request"
    assert result.links.self == "/api/jobs/job_789"
    assert result.links.status == "/api/jobs/job_789/status"
    assert result.links.result == "/api/jobs/job_789/result"


def test_build_create_job_response_normalizes_naive_datetime_to_utc():
    created_at = datetime(2026, 3, 19, 12, 0, 0)

    result = build_create_job_response(
        job_id="job_999",
        correlation_id="corr_999",
        created_at=created_at,
        schema_version="v1",
    )

    assert result.created_at == "2026-03-19T12:00:00Z"
    assert result.message_type == "job.request"
# api/mappers/job_response_mapper.py

from __future__ import annotations

from datetime import datetime, timezone

from api.models.job import CreateJobResponse, JobLinks, JobState


def build_create_job_response(
    *,
    job_id: str,
    correlation_id: str,
    created_at: datetime | str,
    schema_version: str,
    base_path: str = "/v1/jobs",
) -> CreateJobResponse:
    job_path = f"{base_path.rstrip('/')}/{job_id}"

    return CreateJobResponse(
        job_id=job_id,
        correlation_id=correlation_id,
        state=JobState.QUEUED,
        created_at=_to_iso8601(created_at),
        message_type="job.request",
        schema_version=schema_version,
        links=JobLinks(
            self=job_path,
            status=f"{job_path}/status",
            result=f"{job_path}/result",
        ),
    )


def _to_iso8601(value: datetime | str) -> str:
    if isinstance(value, str):
        return value

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)

    return value.isoformat().replace("+00:00", "Z")
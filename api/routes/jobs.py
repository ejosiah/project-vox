# api/routes/jobs.py

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, status

from api.mappers.job_request_mapper import map_create_job_request
from api.mappers.job_response_mapper import build_create_job_response
from api.models.job import CreateJobRequest, CreateJobResponse


def build_jobs_router(*, job_service, schema_version: str) -> APIRouter:
    router = APIRouter(prefix="/v1/jobs", tags=["jobs"])

    @router.post(
        "",
        response_model=CreateJobResponse,
        status_code=status.HTTP_201_CREATED,
    )
    def create_job(request: CreateJobRequest) -> CreateJobResponse:
        job_id = str(uuid4())
        correlation_id = str(uuid4())
        created_at = datetime.now(timezone.utc)

        job_request = map_create_job_request(
            request,
            schema_version=schema_version,
            job_id=job_id,
            correlation_id=correlation_id,
            created_at=created_at,
        )

        job_service.create_job(job_request)

        return build_create_job_response(
            job_id=job_id,
            correlation_id=correlation_id,
            created_at=created_at,
            schema_version=schema_version,
        )

    return router
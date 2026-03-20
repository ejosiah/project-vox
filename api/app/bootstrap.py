# api/app/bootstrap.py

from __future__ import annotations

import logging

from confluent_kafka import Producer
from fastapi import FastAPI

from api.app.settings import AppSettings
from api.routes.jobs import build_jobs_router
from api.services.job_service import JobService

LOGGER = logging.getLogger(__name__)


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def build_producer(settings: AppSettings) -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
        }
    )


def build_job_service(settings: AppSettings) -> JobService:
    producer = build_producer(settings)
    return JobService(producer=producer, config=settings.kafka)


def build_app(settings: AppSettings) -> FastAPI:
    app = FastAPI(title="project-vox-api")

    job_service = build_job_service(settings)

    app.include_router(
        build_jobs_router(
            job_service=job_service,
            schema_version=settings.api.schema_version,
        )
    )

    return app
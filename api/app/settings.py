# api/app/settings.py

from __future__ import annotations

import os

from dataclasses import dataclass


@dataclass(slots=True)
class KafkaSettings:
    bootstrap_servers: str
    topic: str


@dataclass(slots=True)
class ApiSettings:
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"
    schema_version: str = "v1"


@dataclass(slots=True)
class AppSettings:
    kafka: KafkaSettings
    api: ApiSettings


def load_settings() -> AppSettings:
    return AppSettings(
        kafka=KafkaSettings(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092"),
            topic=os.getenv("KAFKA_JOB_REQUEST_TOPIC", "vox.jobs.request"),
        ),
        api=ApiSettings(
            host=os.getenv("API_HOST", "0.0.0.0"),
            port=int(os.getenv("API_PORT", "8000")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            schema_version=os.getenv("SCHEMA_VERSION", "v1"),
        ),
    )
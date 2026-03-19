# app/settings.py

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True)
class KafkaSettings:
    bootstrap_servers: str
    input_topic: str
    output_topic: str
    group_id: str = "project-vox-worker"


@dataclass(slots=True)
class UploadSettings:
    strategy: str
    s3_bucket: str | None = None
    s3_prefix: str = "jobs"
    staging_dir: str | None = None
    download_base_url: str | None = None


@dataclass(slots=True)
class AppSettings:
    kafka: KafkaSettings
    upload: UploadSettings
    workspace_root: str
    log_level: str = "INFO"


def load_settings() -> AppSettings:
    return AppSettings(
        kafka=KafkaSettings(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092,kafka-broker-2:9094,kafka-broker-3:9096"),
            input_topic=os.getenv("KAFKA_INPUT_TOPIC", "vox.jobs.request"),
            output_topic=os.getenv("KAFKA_OUTPUT_TOPIC", "vox.jobs.result"),
            group_id=os.getenv("KAFKA_GROUP_ID", "project-vox-worker"),
        ),
        upload=UploadSettings(
            strategy=os.getenv("UPLOAD_STRATEGY", "staging_folder"),
            s3_bucket=os.getenv("UPLOAD_S3_BUCKET"),
            s3_prefix=os.getenv("UPLOAD_S3_PREFIX", "vox/jobs"),
            staging_dir=os.getenv("UPLOAD_STAGING_DIR", "/tmp/vox/staging"),
            download_base_url=os.getenv("UPLOAD_DOWNLOAD_BASE_URL", "http://example.com/downloads"),
        ),
        workspace_root=os.getenv("WORKSPACE_ROOT", "/tmp/vox/jobs"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
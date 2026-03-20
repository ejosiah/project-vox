# api/services/job_service.py

from __future__ import annotations

from confluent_kafka import Producer
from vox.job_request_pb2 import JobRequest


class JobService:
    def __init__(self, producer: Producer, config) -> None:
        self._producer = producer
        self._config = config

    def create_job(self, job_request: JobRequest) -> None:
        topic = self._config.topic

        # Serialize protobuf to bytes
        payload = job_request.SerializeToString()
        key = job_request.job_id.encode("utf-8")

        self._producer.produce(
            topic=topic,
            key=key,
            value=payload,
        )

        # ensure delivery (simple for now)
        self._producer.flush()
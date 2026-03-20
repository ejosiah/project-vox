# tests/api/services/job_service_test.py

from __future__ import annotations

from unittest.mock import Mock

from api.services.job_service import JobService
from vox.job_request_pb2 import JobRequest


class DummyConfig:
    def __init__(self, topic: str):
        self.topic = topic


def test_create_job_publishes_to_kafka_with_job_id_key():
    producer = Mock()
    config = DummyConfig(topic="vox.jobs.request")

    service = JobService(producer, config)

    job_request = JobRequest(job_id="job-123")

    service.create_job(job_request)

    producer.produce.assert_called_once()

    kwargs = producer.produce.call_args.kwargs

    assert kwargs["topic"] == "vox.jobs.request"
    assert kwargs["key"] == b"job-123"
    assert isinstance(kwargs["value"], bytes)


def test_create_job_flushes_producer():
    producer = Mock()
    config = DummyConfig(topic="vox.jobs.request")

    service = JobService(producer, config)
    job_request = JobRequest(job_id="job-123")

    service.create_job(job_request)

    producer.flush.assert_called_once()
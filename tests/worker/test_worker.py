# tests/worker/test_worker.py

from __future__ import annotations

from unittest.mock import Mock

from core.pipeline.context import JobContext
from core.worker.worker import Worker
from vox.job_request_pb2 import JobRequest


def test_worker_processes_success_and_publishes_result():
    consumer = Mock()
    producer = Mock()
    job_runner = Mock()

    request = JobRequest()
    request.schema_version = "1.0"
    request.job_id = "job-123"
    request.correlation_id = "corr-123"
    request.source.local_file.path = "/tmp/input.mp4"

    message = Mock()
    message.value = b"raw"
    consumer.poll.return_value = {"topic": [message]}

    def deserialize(_: bytes) -> JobRequest:
        return request

    def serialize(result_message) -> bytes:
        return result_message.SerializeToString()

    def run_side_effect(context: JobContext):
        context.metadata["job_outcome"] = {"status": "success"}

    job_runner.run.side_effect = run_side_effect

    worker = Worker(
        consumer=consumer,
        producer=producer,
        job_runner=job_runner,
        request_deserializer=deserialize,
        result_serializer=serialize,
        input_topic="vox.jobs.request",
        output_topic="vox.jobs.result",
    )

    worker.run_once()

    job_runner.run.assert_called_once()
    producer.send.assert_called_once()
    producer.flush.assert_called_once()
    consumer.commit.assert_called_once()


def test_worker_processes_failure_and_still_publishes_result():
    consumer = Mock()
    producer = Mock()
    job_runner = Mock()

    request = JobRequest()
    request.schema_version = "1.0"
    request.job_id = "job-456"
    request.correlation_id = "corr-456"
    request.source.local_file.path = "/tmp/input.mp4"

    message = Mock()
    message.value = b"raw"
    consumer.poll.return_value = {"topic": [message]}

    job_runner.run.side_effect = RuntimeError("pipeline failed")

    worker = Worker(
        consumer=consumer,
        producer=producer,
        job_runner=job_runner,
        request_deserializer=lambda _: request,
        result_serializer=lambda msg: msg.SerializeToString(),
        input_topic="vox.jobs.request",
        output_topic="vox.jobs.result",
    )

    worker.run_once()

    job_runner.run.assert_called_once()
    producer.send.assert_called_once()
    producer.flush.assert_called_once()
    consumer.commit.assert_called_once()
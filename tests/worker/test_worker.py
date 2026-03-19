# tests/worker/test_worker.py

from __future__ import annotations

from unittest.mock import Mock

from core.pipeline.context import JobContext
from core.worker.worker import Worker
from vox.job_request_pb2 import JobRequest


def _build_request(job_id: str, correlation_id: str) -> JobRequest:
    request = JobRequest()
    request.schema_version = "1.0"
    request.job_id = job_id
    request.correlation_id = correlation_id
    request.source.local_file.path = "/tmp/input.mp4"
    return request


def test_worker_processes_success_and_publishes_result():
    consumer = Mock()
    producer = Mock()
    job_runner = Mock()

    request = _build_request("job-123", "corr-123")

    message = Mock()
    message.value.return_value = b"raw"
    message.error.return_value = None

    consumer.poll.return_value = message

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
    producer.produce.assert_called_once()
    producer.flush.assert_called_once()
    consumer.commit.assert_called_once_with(message=message)


def test_worker_processes_failure_and_still_publishes_result():
    consumer = Mock()
    producer = Mock()
    job_runner = Mock()

    request = _build_request("job-456", "corr-456")

    message = Mock()
    message.value.return_value = b"raw"
    message.error.return_value = None

    consumer.poll.return_value = message
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
    producer.produce.assert_called_once()
    producer.flush.assert_called_once()
    consumer.commit.assert_called_once_with(message=message)


def test_worker_returns_when_poll_has_no_message():
    consumer = Mock()
    producer = Mock()
    job_runner = Mock()

    consumer.poll.return_value = None

    worker = Worker(
        consumer=consumer,
        producer=producer,
        job_runner=job_runner,
        request_deserializer=lambda _: None,
        result_serializer=lambda msg: msg.SerializeToString(),
        input_topic="vox.jobs.request",
        output_topic="vox.jobs.result",
    )

    worker.run_once()

    job_runner.run.assert_not_called()
    producer.produce.assert_not_called()
    producer.flush.assert_not_called()
    consumer.commit.assert_not_called()


def test_worker_ignores_message_with_error():
    consumer = Mock()
    producer = Mock()
    job_runner = Mock()

    message = Mock()
    message.error.return_value = Mock()

    consumer.poll.return_value = message

    worker = Worker(
        consumer=consumer,
        producer=producer,
        job_runner=job_runner,
        request_deserializer=lambda _: None,
        result_serializer=lambda msg: msg.SerializeToString(),
        input_topic="vox.jobs.request",
        output_topic="vox.jobs.result",
    )

    worker.run_once()

    job_runner.run.assert_not_called()
    producer.produce.assert_not_called()
    producer.flush.assert_not_called()
    consumer.commit.assert_not_called()
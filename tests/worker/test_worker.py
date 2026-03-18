# tests/worker/test_kafka_worker.py

from __future__ import annotations

from unittest.mock import Mock

from core.pipeline.context import JobContext
from core.pipeline.stage import StageResult, StageStatus
from core.worker.worker import Worker


class DummyRequest:
    def __init__(self, job_id: str, output_types=None):
        self.job_id = job_id
        self.output_types = output_types or []


class DummyUploadedOutput:
    def __init__(self, destination: str, download_url: str | None = None):
        self.destination = destination
        self.download_url = download_url


class DummyUploadResult:
    def __init__(self, outputs):
        self.outputs = outputs


def test_kafka_worker_processes_message_and_sends_result():
    consumer = Mock()
    producer = Mock()
    job_runner = Mock()

    request = DummyRequest("job-123", [])
    message = Mock()
    message.value = b"raw"

    consumer.poll.return_value = {"topic": [message]}

    def deserializer(_):
        return request

    def serializer(msg):
        return b"serialized"

    context = JobContext(job_id="job-123", request=request, metadata={})
    context.metadata["job_outcome"] = {"status": "success"}
    context.metadata["uploaded_outputs"] = DummyUploadResult(
        {
            "txt": DummyUploadedOutput(
                destination="/tmp/file.txt",
                download_url="http://download/file.txt",
            )
        }
    )

    job_runner.run.return_value = StageResult(
        status=StageStatus.SUCCESS,
        context=context,
        metadata={},
    )

    worker = Worker(
        consumer=consumer,
        producer=producer,
        job_runner=job_runner,
        request_deserializer=deserializer,
        result_serializer=serializer,
        input_topic="in",
        output_topic="out",
    )

    worker.run_once()

    job_runner.run.assert_called_once()
    producer.send.assert_called_once()
    producer.flush.assert_called_once()


def test_kafka_worker_handles_empty_poll():
    consumer = Mock()
    producer = Mock()
    job_runner = Mock()

    consumer.poll.return_value = {}

    worker = Worker(
        consumer=consumer,
        producer=producer,
        job_runner=job_runner,
        request_deserializer=lambda x: x,
        result_serializer=lambda x: x,
        input_topic="in",
        output_topic="out",
    )

    worker.run_once()

    job_runner.run.assert_not_called()
    producer.send.assert_not_called()


def test_kafka_worker_maps_output_types_from_enum():
    class EnumMock:
        name = "OUTPUT_TYPE_JSON"

    worker = Worker(
        consumer=Mock(),
        producer=Mock(),
        job_runner=Mock(),
        request_deserializer=lambda x: x,
        result_serializer=lambda x: x,
        input_topic="in",
        output_topic="out",
    )

    mapped = worker._map_output_type(EnumMock())
    assert mapped == "json"


def test_kafka_worker_serializes_uploaded_outputs():
    worker = Worker(
        consumer=Mock(),
        producer=Mock(),
        job_runner=Mock(),
        request_deserializer=lambda x: x,
        result_serializer=lambda x: x,
        input_topic="in",
        output_topic="out",
    )

    uploaded = DummyUploadResult(
        {
            "txt": DummyUploadedOutput("dest", "url"),
            "json": DummyUploadedOutput("dest2", None),
        }
    )

    result = worker._serialize_outputs(uploaded)

    assert result["txt"]["destination"] == "dest"
    assert result["txt"]["download_url"] == "url"
    assert result["json"]["destination"] == "dest2"
    assert result["json"]["download_url"] is None


def test_kafka_worker_handles_missing_uploaded_outputs():
    worker = Worker(
        consumer=Mock(),
        producer=Mock(),
        job_runner=Mock(),
        request_deserializer=lambda x: x,
        result_serializer=lambda x: x,
        input_topic="in",
        output_topic="out",
    )

    result = worker._serialize_outputs(None)

    assert result == {}
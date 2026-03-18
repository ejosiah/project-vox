# tests/worker/test_message_mapper.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from core.worker.message_mapper import MessageMapper
from core.pipeline.context import JobContext
from vox.common_pb2 import JOB_STATE_FAILED, JOB_STATE_SUCCEEDED, OUTPUT_TYPE_JSON, OUTPUT_TYPE_TXT
from vox.job_request_pb2 import JobRequest


@dataclass
class DummyUploadedOutput:
    destination: str
    download_url: str | None = None


@dataclass
class DummyUploadResult:
    outputs: dict[str, DummyUploadedOutput]


def test_request_to_context_maps_basic_fields():
    request = JobRequest()
    request.schema_version = "1.0"
    request.job_id = "job-123"
    request.correlation_id = "corr-123"
    request.source.local_file.path = "/tmp/input.mp4"
    request.output_types.extend([OUTPUT_TYPE_TXT, OUTPUT_TYPE_JSON])

    item = request.metadata.add()
    item.key = "tenant_id"
    item.value = "abc"

    mapper = MessageMapper()
    context = mapper.request_to_context(request)

    assert context.job_id == "job-123"
    assert context.request["correlation_id"] == "corr-123"
    assert context.request["source"] == "/tmp/input.mp4"
    assert context.output_types == ["txt", "json"]
    assert context.metadata["tenant_id"] == "abc"


def test_context_to_result_maps_success_outputs():
    context = JobContext(
        job_id="job-123",
        request={"correlation_id": "corr-123"},
        metadata={
            "started_at": datetime(2026, 3, 18, 12, 0, tzinfo=timezone.utc).isoformat(),
            "uploaded_outputs": DummyUploadResult(
                outputs={
                    "txt": DummyUploadedOutput(
                        destination="/tmp/transcript.txt",
                        download_url="https://example.com/transcript.txt",
                    )
                }
            ),
        },
    )

    mapper = MessageMapper()
    result = mapper.context_to_result(context=context, success=True)

    assert result.job_id == "job-123"
    assert result.correlation_id == "corr-123"
    assert result.state == JOB_STATE_SUCCEEDED
    assert len(result.outputs) == 1
    assert result.outputs[0].output_type == OUTPUT_TYPE_TXT
    assert result.outputs[0].destination == "/tmp/transcript.txt"
    assert result.outputs[0].download_url == "https://example.com/transcript.txt"


def test_context_to_result_maps_failure():
    context = JobContext(
        job_id="job-999",
        request={"correlation_id": "corr-999"},
        metadata={"failed_stage": "generate_output"},
    )

    mapper = MessageMapper()
    result = mapper.context_to_result(
        context=context,
        success=False,
        error=RuntimeError("boom"),
    )

    assert result.job_id == "job-999"
    assert result.state == JOB_STATE_FAILED
    assert result.error.stage == "generate_output"
    assert result.error.type == "RuntimeError"
    assert result.error.message == "boom"
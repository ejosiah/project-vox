# core/worker/message_mapper.py

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from google.protobuf.timestamp_pb2 import Timestamp

from core.pipeline.context import JobContext
from vox.common_pb2 import (
    JOB_STATE_FAILED,
    JOB_STATE_SUCCEEDED,
    OUTPUT_TYPE_JSON,
    OUTPUT_TYPE_SRT,
    OUTPUT_TYPE_TXT,
    OUTPUT_TYPE_VTT,
    KeyValue,
)
from vox.job_request_pb2 import JobRequest
from vox.job_result_pb2 import JobError, JobResult, OutputArtifact, TimingInfo


class MessageMapper:
    OUTPUT_TYPE_MAP = {
        OUTPUT_TYPE_TXT: "txt",
        OUTPUT_TYPE_JSON: "json",
        OUTPUT_TYPE_SRT: "srt",
        OUTPUT_TYPE_VTT: "vtt",
    }

    OUTPUT_TYPE_REVERSE_MAP = {
        "txt": OUTPUT_TYPE_TXT,
        "json": OUTPUT_TYPE_JSON,
        "srt": OUTPUT_TYPE_SRT,
        "vtt": OUTPUT_TYPE_VTT,
    }

    def request_to_context(self, request: JobRequest) -> JobContext:
        metadata = {item.key: item.value for item in request.metadata}

        context = JobContext(
            job_id=request.job_id,
            request={
                "job_id": request.job_id,
                "correlation_id": request.correlation_id,
                "source": self._extract_source(request),
                "schema_version": request.schema_version,
            },
            metadata=metadata,
        )

        context.output_types = [
            self.OUTPUT_TYPE_MAP[output_type]
            for output_type in request.output_types
            if output_type in self.OUTPUT_TYPE_MAP
        ]

        return context

    def context_to_result(
        self,
        *,
        context: JobContext,
        success: bool,
        error: Exception | None = None,
    ) -> JobResult:
        result = JobResult()
        result.schema_version = "1.0"
        result.job_id = context.job_id
        result.correlation_id = str(context.request.get("correlation_id", ""))

        completed_at = Timestamp()
        completed_at.FromDatetime(datetime.now(timezone.utc))
        result.completed_at.CopyFrom(completed_at)

        result.state = JOB_STATE_SUCCEEDED if success else JOB_STATE_FAILED

        uploaded_outputs = context.metadata.get("uploaded_outputs")
        if uploaded_outputs is not None and hasattr(uploaded_outputs, "outputs"):
            for output_type, uploaded in uploaded_outputs.outputs.items():
                artifact = OutputArtifact()
                artifact.output_type = self.OUTPUT_TYPE_REVERSE_MAP.get(output_type, 0)
                artifact.destination = str(getattr(uploaded, "destination", ""))
                artifact.download_url = str(getattr(uploaded, "download_url", ""))
                result.outputs.append(artifact)

        timing = TimingInfo()
        started_at = self._maybe_timestamp(context.metadata.get("started_at"))
        if started_at is not None:
            timing.started_at.CopyFrom(started_at)
        timing.completed_at.CopyFrom(completed_at)
        result.timing.CopyFrom(timing)

        if error is not None:
            job_error = JobError()
            job_error.stage = str(context.metadata.get("failed_stage", ""))
            job_error.type = type(error).__name__
            job_error.message = str(error)
            result.error.CopyFrom(job_error)

        for key, value in context.metadata.items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                item = KeyValue()
                item.key = str(key)
                item.value = "" if value is None else str(value)
                result.metadata.append(item)

        return result

    @staticmethod
    def _extract_source(request: JobRequest) -> str:
        source = request.source

        match source.WhichOneof("location"):
            case "local_file":
                return source.local_file.path
            case "http":
                return source.http.url
            case "s3":
                return f"s3://{source.s3.bucket}/{source.s3.key}"
            case _:
                return ""

    @staticmethod
    def _maybe_timestamp(value: Any) -> Timestamp | None:
        if not value:
            return None

        if isinstance(value, datetime):
            ts = Timestamp()
            ts.FromDatetime(value.astimezone(timezone.utc))
            return ts

        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
            ts = Timestamp()
            ts.FromDatetime(dt.astimezone(timezone.utc))
            return ts

        return None
# api/mappers/job_request_mapper.py

from __future__ import annotations

from datetime import datetime, timezone

from google.protobuf.timestamp_pb2 import Timestamp

from api.models.job import CreateJobRequest, OutputType, SourceKind
from vox.common_pb2 import KeyValue
from vox.job_request_pb2 import (
    DiarizationOptions,
    HttpSource,
    JobRequest,
    LocalFileSource,
    ProcessingOptions,
    S3Source,
    Source,
    TranscriptionOptions,
)


class JobRequestMappingError(ValueError):
    """Raised when a CreateJobRequest cannot be mapped to JobRequest."""


def map_create_job_request(
    request: CreateJobRequest,
    *,
    schema_version: str,
    job_id: str,
    correlation_id: str,
    created_at: datetime | str,
) -> JobRequest:
    return JobRequest(
        schema_version=schema_version,
        job_id=job_id,
        correlation_id=correlation_id,
        created_at=_to_timestamp(created_at),
        source=_map_source(request),
        output_types=[_map_output_type(output_type) for output_type in request.output_types],
        options=_map_processing_options(request),
        metadata=[KeyValue(key=item.key, value=item.value) for item in request.metadata],
    )


def _to_timestamp(value: datetime | str) -> Timestamp:
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    else:
        dt = value

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    timestamp = Timestamp()
    timestamp.FromDatetime(dt)
    return timestamp


def _map_source(request: CreateJobRequest) -> Source:
    source = request.source

    if source.kind == SourceKind.LOCAL_FILE:
        if source.local_file is None:
            raise JobRequestMappingError("source.local_file is required for LOCAL_FILE source kind")
        return Source(
            local_file=LocalFileSource(
                path=source.local_file.path,
            )
        )

    if source.kind == SourceKind.HTTP_URL:
        if source.http is None:
            raise JobRequestMappingError("source.http is required for HTTP_URL source kind")
        return Source(
            http=HttpSource(
                url=source.http.url,
            )
        )

    if source.kind == SourceKind.S3_OBJECT:
        if source.s3 is None:
            raise JobRequestMappingError("source.s3 is required for S3_OBJECT source kind")
        return Source(
            s3=S3Source(
                bucket=source.s3.bucket,
                key=source.s3.key,
                region=source.s3.region,
            )
        )

    raise JobRequestMappingError(f"unsupported source kind: {source.kind!r}")


def _map_output_type(output_type: OutputType) -> int:
    mapping = {
        OutputType.TXT: 1,
        OutputType.JSON: 2,
        OutputType.SRT: 3,
        OutputType.VTT: 4,
    }

    try:
        return mapping[output_type]
    except KeyError as exc:
        raise JobRequestMappingError(f"unsupported output type: {output_type!r}") from exc


def _map_processing_options(request: CreateJobRequest) -> ProcessingOptions:
    options = request.options
    if options is None:
        return ProcessingOptions()

    transcription = TranscriptionOptions()
    if options.transcription is not None:
        if options.transcription.model_name is not None:
            transcription.model_name = options.transcription.model_name
        if options.transcription.language is not None:
            transcription.language = options.transcription.language
        if options.transcription.vad_filter is not None:
            transcription.vad_filter = options.transcription.vad_filter

    diarization = DiarizationOptions()
    if options.diarization is not None:
        diarization.enabled = options.diarization.enabled
        if options.diarization.num_speakers is not None:
            diarization.num_speakers = options.diarization.num_speakers
        if options.diarization.min_speakers is not None:
            diarization.min_speakers = options.diarization.min_speakers
        if options.diarization.max_speakers is not None:
            diarization.max_speakers = options.diarization.max_speakers

    return ProcessingOptions(
        transcription=transcription,
        diarization=diarization,
    )
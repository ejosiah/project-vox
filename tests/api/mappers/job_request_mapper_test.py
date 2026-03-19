# tests/api/mappers/job_request_mapper_test.py

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from api.mappers.job_request_mapper import (
    JobRequestMappingError,
    map_create_job_request,
)
from api.models.job import CreateJobRequest


def test_map_create_job_request_http_source():
    request = CreateJobRequest.model_validate(
        {
            "source": {
                "kind": "HTTP_URL",
                "http": {
                    "url": "https://example.com/audio.mp3",
                },
            },
            "output_types": ["TXT", "SRT", "JSON"],
            "options": {
                "transcription": {
                    "model_name": "whisper-large-v3",
                    "language": "en",
                    "vad_filter": True,
                },
                "diarization": {
                    "enabled": True,
                    "num_speakers": 2,
                },
            },
            "metadata": [
                {"key": "tenant_id", "value": "acme"},
                {"key": "requested_by", "value": "api"},
            ],
        }
    )

    created_at = datetime(2026, 3, 19, 12, 0, 0, tzinfo=timezone.utc)

    result = map_create_job_request(
        request,
        schema_version="v1",
        job_id="job_123",
        correlation_id="corr_123",
        created_at=created_at,
    )

    assert result.schema_version == "v1"
    assert result.job_id == "job_123"
    assert result.correlation_id == "corr_123"
    assert result.created_at.ToDatetime().replace(tzinfo=timezone.utc) == created_at

    assert result.source.WhichOneof("location") == "http"
    assert result.source.http.url == "https://example.com/audio.mp3"

    assert list(result.output_types) == [1, 3, 2]

    assert result.options.transcription.model_name == "whisper-large-v3"
    assert result.options.transcription.language == "en"
    assert result.options.transcription.vad_filter is True

    assert result.options.diarization.enabled is True
    assert result.options.diarization.num_speakers == 2
    assert result.options.diarization.min_speakers == 0
    assert result.options.diarization.max_speakers == 0

    assert [(item.key, item.value) for item in result.metadata] == [
        ("tenant_id", "acme"),
        ("requested_by", "api"),
    ]


def test_map_create_job_request_local_file_source():
    request = CreateJobRequest.model_validate(
        {
            "source": {
                "kind": "LOCAL_FILE",
                "local_file": {
                    "path": "/tmp/audio.wav",
                },
            },
            "output_types": ["VTT"],
        }
    )

    result = map_create_job_request(
        request,
        schema_version="v1",
        job_id="job_local",
        correlation_id="corr_local",
        created_at="2026-03-19T12:00:00Z",
    )

    assert result.source.WhichOneof("location") == "local_file"
    assert result.source.local_file.path == "/tmp/audio.wav"
    assert list(result.output_types) == [4]


def test_map_create_job_request_s3_source():
    request = CreateJobRequest.model_validate(
        {
            "source": {
                "kind": "S3_OBJECT",
                "s3": {
                    "bucket": "input-bucket",
                    "key": "media/file.mp4",
                    "region": "eu-west-2",
                },
            },
            "output_types": ["TXT"],
        }
    )

    result = map_create_job_request(
        request,
        schema_version="v1",
        job_id="job_s3",
        correlation_id="corr_s3",
        created_at="2026-03-19T12:00:00Z",
    )

    assert result.source.WhichOneof("location") == "s3"
    assert result.source.s3.bucket == "input-bucket"
    assert result.source.s3.key == "media/file.mp4"
    assert result.source.s3.region == "eu-west-2"


def test_map_create_job_request_without_options_or_metadata():
    request = CreateJobRequest.model_validate(
        {
            "source": {
                "kind": "HTTP_URL",
                "http": {
                    "url": "https://example.com/audio.mp3",
                },
            },
            "output_types": ["TXT"],
        }
    )

    result = map_create_job_request(
        request,
        schema_version="v1",
        job_id="job_minimal",
        correlation_id="corr_minimal",
        created_at="2026-03-19T12:00:00Z",
    )

    assert result.options.transcription.model_name == ""
    assert result.options.transcription.language == ""
    assert result.options.transcription.vad_filter is False

    assert result.options.diarization.enabled is False
    assert result.options.diarization.num_speakers == 0
    assert result.options.diarization.min_speakers == 0
    assert result.options.diarization.max_speakers == 0

    assert list(result.metadata) == []


def test_map_create_job_request_accepts_iso_string_created_at():
    request = CreateJobRequest.model_validate(
        {
            "source": {
                "kind": "HTTP_URL",
                "http": {
                    "url": "https://example.com/audio.mp3",
                },
            },
            "output_types": ["TXT"],
        }
    )

    result = map_create_job_request(
        request,
        schema_version="v1",
        job_id="job_time",
        correlation_id="corr_time",
        created_at="2026-03-19T12:00:00Z",
    )

    dt = result.created_at.ToDatetime().replace(tzinfo=timezone.utc)
    assert dt == datetime(2026, 3, 19, 12, 0, 0, tzinfo=timezone.utc)


def test_create_job_request_validation_rejects_mismatched_source_payload():
    with pytest.raises(ValidationError, match="http is required when kind=HTTP_URL"):
        CreateJobRequest.model_validate(
            {
                "source": {
                    "kind": "HTTP_URL",
                },
                "output_types": ["TXT"],
            }
        )


def test_create_job_request_validation_rejects_multiple_source_payloads():
    with pytest.raises(ValidationError, match="only http may be set when kind=HTTP_URL"):
        CreateJobRequest.model_validate(
            {
                "source": {
                    "kind": "HTTP_URL",
                    "http": {
                        "url": "https://example.com/audio.mp3",
                    },
                    "s3": {
                        "bucket": "input-bucket",
                        "key": "media/file.mp4",
                        "region": "eu-west-2",
                    },
                },
                "output_types": ["TXT"],
            }
        )


def test_create_job_request_validation_rejects_empty_output_types():
    with pytest.raises(ValidationError):
        CreateJobRequest.model_validate(
            {
                "source": {
                    "kind": "HTTP_URL",
                    "http": {
                        "url": "https://example.com/audio.mp3",
                    },
                },
                "output_types": [],
            }
        )


def test_create_job_request_validation_rejects_conflicting_diarization_options():
    with pytest.raises(
        ValidationError,
        match="num_speakers cannot be combined with min_speakers or max_speakers",
    ):
        CreateJobRequest.model_validate(
            {
                "source": {
                    "kind": "HTTP_URL",
                    "http": {
                        "url": "https://example.com/audio.mp3",
                    },
                },
                "output_types": ["TXT"],
                "options": {
                    "diarization": {
                        "enabled": True,
                        "num_speakers": 2,
                        "min_speakers": 1,
                    }
                },
            }
        )


def test_map_create_job_request_raises_for_unsupported_output_type():
    request = CreateJobRequest.model_validate(
        {
            "source": {
                "kind": "HTTP_URL",
                "http": {
                    "url": "https://example.com/audio.mp3",
                },
            },
            "output_types": ["TXT"],
        }
    )

    request.output_types = ["DOCX"]  # type: ignore[assignment]

    with pytest.raises(JobRequestMappingError, match="unsupported output type"):
        map_create_job_request(
            request,
            schema_version="v1",
            job_id="job_bad",
            correlation_id="corr_bad",
            created_at="2026-03-19T12:00:00Z",
        )
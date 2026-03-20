# tests/api/routes/jobs_test.py

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routes.jobs import build_jobs_router


class RecordingJobService:
    def __init__(self) -> None:
        self.calls = []

    def create_job(self, job_request) -> None:
        self.calls.append(job_request)


def test_create_job_maps_request_publishes_to_service_and_returns_response():
    job_service = RecordingJobService()

    app = FastAPI()
    app.include_router(
        build_jobs_router(
            job_service=job_service,
            schema_version="v1",
        )
    )

    client = TestClient(app)

    response = client.post(
        "/v1/jobs",
        json={
            "source": {
                "kind": "HTTP_URL",
                "http": {
                    "url": "https://example.com/audio.mp3",
                },
            },
            "output_types": ["TXT", "SRT"],
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
            ],
        },
    )

    assert response.status_code == 201

    body = response.json()

    assert body["job_id"]
    assert body["correlation_id"]
    assert body["state"] == "QUEUED"
    assert body["message_type"] == "job.request"
    assert body["schema_version"] == "v1"
    assert body["created_at"]
    assert body["links"]["self"] == f"/v1/jobs/{body['job_id']}"
    assert body["links"]["status"] == f"/v1/jobs/{body['job_id']}/status"
    assert body["links"]["result"] == f"/v1/jobs/{body['job_id']}/result"

    assert len(job_service.calls) == 1
    published_job_request = job_service.calls[0]

    assert published_job_request.schema_version == "v1"
    assert published_job_request.job_id == body["job_id"]
    assert published_job_request.correlation_id == body["correlation_id"]
    assert published_job_request.source.WhichOneof("location") == "http"
    assert published_job_request.source.http.url == "https://example.com/audio.mp3"
    assert list(published_job_request.output_types) == [1, 3]
    assert published_job_request.options.transcription.model_name == "whisper-large-v3"
    assert published_job_request.options.transcription.language == "en"
    assert published_job_request.options.transcription.vad_filter is True
    assert published_job_request.options.diarization.enabled is True
    assert published_job_request.options.diarization.num_speakers == 2
    assert [(item.key, item.value) for item in published_job_request.metadata] == [
        ("tenant_id", "acme"),
    ]


def test_create_job_returns_422_for_invalid_request():
    job_service = RecordingJobService()

    app = FastAPI()
    app.include_router(
        build_jobs_router(
            job_service=job_service,
            schema_version="v1",
        )
    )

    client = TestClient(app)

    response = client.post(
        "/v1/jobs",
        json={
            "source": {
                "kind": "HTTP_URL",
            },
            "output_types": ["TXT"],
        },
    )

    assert response.status_code == 422
    assert job_service.calls == []
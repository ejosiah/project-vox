# Project VOX API

The **VOX API** is the public HTTP interface for submitting transcription jobs into the VOX processing pipeline.

It accepts job requests, maps them into the internal protobuf format, publishes them to Kafka, and returns a job creation response.

---

# Overview

Flow:

```
Client → API → Mapper → Kafka → Worker → Pipeline → Result
```

1. Client sends HTTP request (`POST /v1/jobs`)  
2. API validates request using Pydantic models  
3. Request is mapped to `JobRequest` (protobuf)  
4. Message is published to Kafka  
5. Worker consumes and processes the job  
6. API returns a **CreateJobResponse**

---

# Project Structure

```
api/
├── app/
│   ├── bootstrap.py     # Dependency wiring (Kafka, services, routers)
│   ├── main.py          # Entry point (runs FastAPI)
│   └── settings.py      # Environment-driven configuration
│
├── models/
│   └── job.py           # Pydantic request/response models
│
├── mappers/
│   ├── job_request_mapper.py
│   └── job_response_mapper.py
│
├── routes/
│   └── jobs.py          # HTTP endpoints
│
├── services/
│   └── job_service.py   # Kafka publishing logic
```

---

# Running the API

## 1. Install dependencies

```
pip install -r requirements.txt
```

## 2. Set environment variables

```
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_JOB_REQUEST_TOPIC=vox.jobs.request
export API_HOST=0.0.0.0
export API_PORT=8000
export LOG_LEVEL=INFO
export SCHEMA_VERSION=v1
```

## 3. Run the API

```
python -m api.app.main
```

or:

```
uvicorn api.app.main:app --host 0.0.0.0 --port 8000
```

---

# API Endpoints

## Create Job

### `POST /v1/jobs`

### Request

```json
{
  "source": {
    "kind": "HTTP_URL",
    "http": {
      "url": "https://example.com/audio.mp3"
    }
  },
  "output_types": ["TXT", "SRT"],
  "options": {
    "transcription": {
      "model_name": "whisper-large-v3",
      "language": "en",
      "vad_filter": true
    },
    "diarization": {
      "enabled": true,
      "num_speakers": 2
    }
  },
  "metadata": [
    { "key": "tenant_id", "value": "acme" }
  ]
}
```

### Response (201)

```json
{
  "job_id": "uuid",
  "correlation_id": "uuid",
  "state": "QUEUED",
  "created_at": "2026-03-19T12:00:00Z",
  "message_type": "job.request",
  "schema_version": "v1",
  "links": {
    "self": "/v1/jobs/{job_id}",
    "status": "/v1/jobs/{job_id}/status",
    "result": "/v1/jobs/{job_id}/result"
  }
}
```

---

# Key Concepts

## Job Request Mapping

- API accepts clean JSON  
- Mapper converts it into protobuf `JobRequest`  
- Adds:
  - `job_id`
  - `correlation_id`
  - `created_at`
  - `schema_version`

## Kafka Publishing

- Uses `confluent_kafka`  
- Topic defined via config  
- Message key = `job_id`  
- Payload = protobuf binary (`SerializeToString()`)

## Separation of Concerns

- **Models** → validation (Pydantic)  
- **Mappers** → API ↔ Proto conversion  
- **Services** → business logic (Kafka publish)  
- **Routes** → HTTP layer only  
- **App** → wiring & configuration  

---

# Testing

```
pytest
```

---

# Next Steps

- Add `/status` endpoint (consume `JobStatusEvent`)  
- Add `/result` endpoint (consume `JobResult`)  
- Add authentication (API keys / JWT)  
- Add request tracing / logging  
- Add retry + delivery callbacks for Kafka  
- Add idempotency handling  

---

# Notes

- API does **not** process jobs — it only submits them  
- Upload configuration is handled internally (not exposed to API)  
- All protocol definitions live in `proto/vox`  

---

# TL;DR

- FastAPI → validate request  
- Mapper → convert to protobuf  
- Kafka → publish job  
- Response → return job metadata  

That’s it — clean, scalable, and aligned with your worker pipeline.
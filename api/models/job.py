# api/models/job.py

from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class OutputType(str, Enum):
    TXT = "TXT"
    JSON = "JSON"
    SRT = "SRT"
    VTT = "VTT"


class JobState(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class SourceKind(str, Enum):
    LOCAL_FILE = "LOCAL_FILE"
    HTTP_URL = "HTTP_URL"
    S3_OBJECT = "S3_OBJECT"


class KeyValueInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: str
    value: str


class LocalFileSourceInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str


class HttpSourceInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str


class S3SourceInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bucket: str
    key: str
    region: str


class SourceInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: SourceKind
    local_file: Optional[LocalFileSourceInput] = None
    http: Optional[HttpSourceInput] = None
    s3: Optional[S3SourceInput] = None

    @model_validator(mode="after")
    def validate_location_matches_kind(self) -> "SourceInput":
        if self.kind == SourceKind.LOCAL_FILE:
            if self.local_file is None:
                raise ValueError("local_file is required when kind=LOCAL_FILE")
            if self.http is not None or self.s3 is not None:
                raise ValueError("only local_file may be set when kind=LOCAL_FILE")

        elif self.kind == SourceKind.HTTP_URL:
            if self.http is None:
                raise ValueError("http is required when kind=HTTP_URL")
            if self.local_file is not None or self.s3 is not None:
                raise ValueError("only http may be set when kind=HTTP_URL")

        elif self.kind == SourceKind.S3_OBJECT:
            if self.s3 is None:
                raise ValueError("s3 is required when kind=S3_OBJECT")
            if self.local_file is not None or self.http is not None:
                raise ValueError("only s3 may be set when kind=S3_OBJECT")

        return self


class TranscriptionOptionsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: Optional[str] = None
    language: Optional[str] = None
    vad_filter: Optional[bool] = None


class DiarizationOptionsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    num_speakers: Optional[int] = Field(default=None, ge=1)
    min_speakers: Optional[int] = Field(default=None, ge=1)
    max_speakers: Optional[int] = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_speaker_options(self) -> "DiarizationOptionsInput":
        if not self.enabled:
            return self

        if self.num_speakers is not None and (
            self.min_speakers is not None or self.max_speakers is not None
        ):
            raise ValueError(
                "num_speakers cannot be combined with min_speakers or max_speakers when diarization is enabled"
            )

        if self.min_speakers is not None and self.max_speakers is not None:
            if self.min_speakers > self.max_speakers:
                raise ValueError("min_speakers cannot be greater than max_speakers")

        return self


class ProcessingOptionsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transcription: Optional[TranscriptionOptionsInput] = None
    diarization: Optional[DiarizationOptionsInput] = None


class CreateJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: SourceInput
    output_types: List[OutputType] = Field(min_length=1)
    options: Optional[ProcessingOptionsInput] = None
    metadata: List[KeyValueInput] = Field(default_factory=list)


class JobLinks(BaseModel):
    model_config = ConfigDict(extra="forbid")

    self: str
    status: str
    result: str


class CreateJobResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str
    correlation_id: str
    state: JobState
    created_at: str
    message_type: str
    schema_version: str
    links: JobLinks
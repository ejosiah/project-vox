# core/output/uploader.py

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol


@dataclass(slots=True)
class UploadedOutput:
    output_type: str
    source_path: Path
    destination: str
    download_url: str | None = None


@dataclass(slots=True)
class UploadResult:
    job_id: str
    outputs: dict[str, UploadedOutput] = field(default_factory=dict)


class OutputUploader(Protocol):
    def upload(self, *, job_id: str, outputs: dict[str, str]) -> UploadResult:
        """
        Upload generated outputs for a job.

        Args:
            job_id: Unique job identifier.
            outputs: Mapping of output type -> local file path.

        Returns:
            UploadResult containing uploaded locations.
        """
        ...
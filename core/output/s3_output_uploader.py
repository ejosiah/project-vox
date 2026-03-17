# core/output/s3_output_uploader.py

from __future__ import annotations

from pathlib import Path
from typing import Any

from core.output.uploader import OutputUploader, UploadResult, UploadedOutput


class S3OutputUploader:
    def __init__(
        self,
        *,
        bucket_name: str,
        prefix: str = "jobs",
        s3_client: Any | None = None,
    ) -> None:
        self._bucket_name = bucket_name
        self._prefix = prefix.strip("/")

        if s3_client is None:
            import boto3

            s3_client = boto3.client("s3")

        self._s3_client = s3_client

    def upload(self, *, job_id: str, outputs: dict[str, str]) -> UploadResult:
        uploaded: dict[str, UploadedOutput] = {}

        for output_type, file_path in outputs.items():
            source_path = Path(file_path)
            if not source_path.exists():
                raise FileNotFoundError(f"Generated output does not exist: {source_path}")

            key = self._build_key(job_id=job_id, output_type=output_type, source_path=source_path)

            self._s3_client.upload_file(
                str(source_path),
                self._bucket_name,
                key,
            )

            uploaded[output_type] = UploadedOutput(
                output_type=output_type,
                source_path=source_path,
                destination=f"s3://{self._bucket_name}/{key}",
                download_url=None,
            )

        return UploadResult(job_id=job_id, outputs=uploaded)

    def _build_key(self, *, job_id: str, output_type: str, source_path: Path) -> str:
        filename = source_path.name
        if self._prefix:
            return f"{self._prefix}/{job_id}/{filename}"
        return f"{job_id}/{filename}"
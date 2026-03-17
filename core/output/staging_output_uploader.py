# core/output/staging_output_uploader.py

from __future__ import annotations

import shutil
from pathlib import Path

from core.output.uploader import UploadResult, UploadedOutput


class StagingFolderOutputUploader:
    def __init__(
        self,
        *,
        staging_dir: str | Path,
        download_base_url: str,
    ) -> None:
        self._staging_dir = Path(staging_dir)
        self._download_base_url = download_base_url.rstrip("/")

    def upload(self, *, job_id: str, outputs: dict[str, str]) -> UploadResult:
        destination_dir = self._staging_dir / job_id
        destination_dir.mkdir(parents=True, exist_ok=True)

        uploaded: dict[str, UploadedOutput] = {}

        for output_type, file_path in outputs.items():
            source_path = Path(file_path)
            if not source_path.exists():
                raise FileNotFoundError(f"Generated output does not exist: {source_path}")

            destination_path = destination_dir / source_path.name
            shutil.copy2(source_path, destination_path)

            uploaded[output_type] = UploadedOutput(
                output_type=output_type,
                source_path=source_path,
                destination=str(destination_path),
                download_url=f"{self._download_base_url}/{job_id}/{destination_path.name}",
            )

        return UploadResult(job_id=job_id, outputs=uploaded)
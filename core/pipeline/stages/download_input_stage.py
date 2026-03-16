from __future__ import annotations

import shutil
from pathlib import Path
from urllib.parse import urlparse, unquote
from urllib.request import urlretrieve

from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


class DownloadInputStage(Stage):
    """
    Download or copy the source media into the local job workspace input folder.

    Supported inputs:
    - local filesystem paths
    - file:// URIs
    - http:// and https:// URLs
    """

    name = "download_input"

    def run(self, context: JobContext) -> StageResult:
        if not context.input_uri:
            raise ValueError("context.input_uri is required for DownloadInputStage")

        destination_dir = self._resolve_destination_dir(context)
        destination_dir.mkdir(parents=True, exist_ok=True)

        source = str(context.input_uri)
        parsed = urlparse(source)

        if parsed.scheme in ("http", "https"):
            filename = self._filename_from_remote(parsed)
            destination = destination_dir / filename
            urlretrieve(source, destination)
        else:
            source_path = self._resolve_local_source_path(source, parsed)
            if not source_path.exists():
                raise FileNotFoundError(f"Input source does not exist: {source_path}")

            destination = destination_dir / source_path.name
            shutil.copy2(source_path, destination)

        context.input_path = destination

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
            metadata={
                "job_id": context.job_id,
                "input_uri": context.input_uri,
                "input_path": str(destination),
            },
        )

    @staticmethod
    def _resolve_destination_dir(context: JobContext) -> Path:
        job = context.metadata.get("job")
        if job is not None and hasattr(job, "input_dir"):
            return Path(job.input_dir)

        if context.workspace_dir is None:
            raise ValueError(
                "context.workspace_dir is required when no Job is present in metadata"
            )

        return Path(context.workspace_dir) / "input"

    @staticmethod
    def _resolve_local_source_path(source: str, parsed) -> Path:
        if parsed.scheme == "file":
            return Path(unquote(parsed.path))

        if parsed.scheme:
            raise ValueError(f"Unsupported input URI scheme: {parsed.scheme}")

        return Path(source)

    @staticmethod
    def _filename_from_remote(parsed) -> str:
        name = Path(unquote(parsed.path)).name
        if not name:
            raise ValueError("Unable to determine filename from remote input_uri")
        return name
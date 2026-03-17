# scripts/process_job.py

from __future__ import annotations

import argparse
import json
import uuid
import logging

from pathlib import Path
from typing import Any

from core.pipeline.context import JobContext
from core.media.diarizer import Diarizer
from core.media.diarizer_backend import PyannoteDiarizerBackend
from core.media.ffmpeg_utils import FFmpeg
from core.media.transcriber import Transcriber
from core.media.transcriber_backend import FasterWhisperTranscriberBackend
from core.pipeline.job_runner import JobRunner
from core.pipeline.stages.align_transcript_stage import AlignTranscriptStage
from core.pipeline.stages.diarize_audio_stage import DiarizeAudioStage
from core.pipeline.stages.download_input_stage import DownloadInputStage
from core.pipeline.stages.extract_audio_stage import ExtractAudioStage
from core.pipeline.stages.generate_output_stage import GenerateOutputStage
from core.pipeline.stages.merge_transcript_stage import MergeTranscriptStage
from core.pipeline.stages.normalize_audio_stage import NormalizeAudioStage
from core.pipeline.stages.probe_media_stage import ProbeMediaStage
from core.pipeline.stages.transcribe_audio_stage import TranscribeAudioStage
from core.pipeline.stages.validate_job_stage import ValidateJobStage
from core.pipeline.stages.workspace_stage import WorkspaceStage
from core.pipeline.stages.finalize_job_stage import FinalizeJobStage
from core.pipeline.stages.cleanup_stage import CleanupStage
from core.pipeline.stages.upload_output_stage import UploadOutputStage
from core.output.staging_output_uploader import StagingFolderOutputUploader


def generate_job_id() -> str:
    return uuid.uuid4().hex


def build_request(source: str, output_types: list[str]) -> dict[str, Any]:
    return {
        "job_id": generate_job_id(),
        "input_uri": source,
        "output_types": output_types,
    }


def build_runner() -> JobRunner:
    ffmpeg = FFmpeg()

    transcriber = Transcriber(
        backend=FasterWhisperTranscriberBackend(
            model_name="base",
            prefer_gpu=True,
        )
    )

    diarizer = Diarizer(
        backend=PyannoteDiarizerBackend(
            token_env_var="HF_TOKEN",
            prefer_gpu=True,
        )
    )

    uploader = StagingFolderOutputUploader(staging_dir=Path("staging"), download_base_url="https://staging.example.com/")

    stages = [
        ValidateJobStage(),
        WorkspaceStage(),
        DownloadInputStage(),
        ProbeMediaStage(ffmpeg=ffmpeg),
        ExtractAudioStage(ffmpeg=ffmpeg),
        NormalizeAudioStage(ffmpeg=ffmpeg),
        TranscribeAudioStage(transcriber=transcriber),
        DiarizeAudioStage(diarizer=diarizer),
        AlignTranscriptStage(),
        MergeTranscriptStage(),
        GenerateOutputStage(),
        UploadOutputStage(uploader=uploader),
        FinalizeJobStage(),
        CleanupStage(),
    ]

    return JobRunner(stages=stages)


def build_context(request: dict[str, Any]) -> JobContext:
    context = JobContext(
        job_id=request["job_id"],
        request=request,
        metadata={},
    )

    context.output_types = request.get("output_types", ["txt"])

    return context


def main() -> int:

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Process a Project Vox job")
    parser.add_argument(
        "--source",
        required=True,
        help="Path or URL to input media",
    )
    parser.add_argument(
        "--output-types",
        nargs="+",
        default=["txt", "json"],
        help="Output types (e.g. txt json srt vtt)",
    )

    args = parser.parse_args()

    request = build_request(args.source, args.output_types)
    context = build_context(request)
    runner = build_runner()

    result = runner.run(context)

    print("Job completed")
    print(f"job_id={context.job_id}")
    print(f"status={result.status.name}")
    print(json.dumps(_safe_metadata(context.metadata), indent=2, ensure_ascii=False))

    return 0


def _safe_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}

    for key, value in metadata.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            safe[key] = value
        elif isinstance(value, Path):
            safe[key] = str(value)
        elif isinstance(value, dict):
            safe[key] = {str(k): str(v) if isinstance(v, Path) else v for k, v in value.items()}
        else:
            safe[key] = repr(value)

    return safe


if __name__ == "__main__":
    raise SystemExit(main())

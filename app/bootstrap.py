# app/bootstrap.py

from __future__ import annotations

import logging
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

from app.settings import AppSettings
from core.media.diarizer import Diarizer
from core.media.diarizer_backend import PyannoteDiarizerBackend
from core.media.ffmpeg_utils import FFmpeg
from core.media.transcriber import Transcriber
from core.media.transcriber_backend import FasterWhisperTranscriberBackend
from core.output.s3_output_uploader import S3OutputUploader
from core.output.staging_output_uploader import StagingFolderOutputUploader
from core.pipeline.job_runner import JobRunner
from core.pipeline.stages.align_transcript_stage import AlignTranscriptStage
from core.pipeline.stages.cleanup_stage import CleanupStage
from core.pipeline.stages.diarize_audio_stage import DiarizeAudioStage
from core.pipeline.stages.download_input_stage import DownloadInputStage
from core.pipeline.stages.extract_audio_stage import ExtractAudioStage
from core.pipeline.stages.finalize_job_stage import FinalizeJobStage
from core.pipeline.stages.generate_output_stage import GenerateOutputStage
from core.pipeline.stages.merge_transcript_stage import MergeTranscriptStage
from core.pipeline.stages.normalize_audio_stage import NormalizeAudioStage
from core.pipeline.stages.probe_media_stage import ProbeMediaStage
from core.pipeline.stages.transcribe_audio_stage import TranscribeAudioStage
from core.pipeline.stages.upload_output_stage import UploadOutputStage
from core.pipeline.stages.validate_job_stage import ValidateJobStage
from core.pipeline.stages.workspace_stage import WorkspaceStage
from core.pipeline.stages.workspace_stage import WorkspaceStageConfig
from core.worker.worker import Worker


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def build_uploader(settings: AppSettings) -> Any:
    if settings.upload.strategy == "s3":
        if not settings.upload.s3_bucket:
            raise ValueError("UPLOAD_S3_BUCKET is required for s3 upload strategy")

        return S3OutputUploader(
            bucket_name=settings.upload.s3_bucket,
            prefix=settings.upload.s3_prefix,
        )

    if settings.upload.strategy == "staging_folder":
        if not settings.upload.staging_dir:
            raise ValueError("UPLOAD_STAGING_DIR is required for staging_folder strategy")
        if not settings.upload.download_base_url:
            raise ValueError(
                "UPLOAD_DOWNLOAD_BASE_URL is required for staging_folder strategy"
            )

        return StagingFolderOutputUploader(
            staging_dir=settings.upload.staging_dir,
            download_base_url=settings.upload.download_base_url,
        )

    raise ValueError(f"Unsupported upload strategy: {settings.upload.strategy}")


def build_job_runner(settings: AppSettings) -> JobRunner:
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

    uploader = build_uploader(settings)

    stages = [
        ValidateJobStage(),
        WorkspaceStage(config=WorkspaceStageConfig(base_dir=settings.workspace_root)),
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


def build_consumer(settings: AppSettings) -> KafkaConsumer:
    return KafkaConsumer(
        settings.kafka.input_topic,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=settings.kafka.group_id,
        enable_auto_commit=False,
        value_deserializer=lambda value: value,
    )


def build_producer(settings: AppSettings) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        value_serializer=lambda value: value,
    )


def deserialize_job_request(payload: bytes) -> Any:
    # Replace with protobuf deserialization once generated classes are wired in.
    from vox.job_request_pb2 import JobRequest

    message = JobRequest()
    message.ParseFromString(payload)
    return message


def serialize_job_result(message: Any) -> bytes:
    # Replace/keep once generated protobuf result mapping is in place.
    return message.SerializeToString()


def build_worker(settings: AppSettings) -> Worker:
    return Worker(
        consumer=build_consumer(settings),
        producer=build_producer(settings),
        job_runner=build_job_runner(settings),
        request_deserializer=deserialize_job_request,
        result_serializer=serialize_job_result,
        input_topic=settings.kafka.input_topic,
        output_topic=settings.kafka.output_topic,
    )
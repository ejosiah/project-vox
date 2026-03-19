# scripts/submit_job.py

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from google.protobuf.timestamp_pb2 import Timestamp

from vox.common_pb2 import OUTPUT_TYPE_JSON, OUTPUT_TYPE_SRT, OUTPUT_TYPE_TXT, OUTPUT_TYPE_VTT
from vox.job_request_pb2 import JobRequest


OUTPUT_TYPE_MAP = {
    "txt": OUTPUT_TYPE_TXT,
    "json": OUTPUT_TYPE_JSON,
    "srt": OUTPUT_TYPE_SRT,
    "vtt": OUTPUT_TYPE_VTT,
}


def build_job_request(video_file: str, output_types: list[str]) -> JobRequest:
    path = Path(video_file)
    if not path.exists():
        raise FileNotFoundError(f"Input file does not exist: {path}")

    normalized_output_types: list[int] = []
    for output_type in output_types:
        key = output_type.strip().lower()
        if key not in OUTPUT_TYPE_MAP:
            raise ValueError(f"Unsupported output type: {output_type}")
        normalized_output_types.append(OUTPUT_TYPE_MAP[key])

    job_id = uuid.uuid4().hex
    correlation_id = uuid.uuid4().hex

    request = JobRequest()
    request.schema_version = "1.0"
    request.job_id = job_id
    request.correlation_id = correlation_id

    created_at = Timestamp()
    created_at.FromDatetime(datetime.now(timezone.utc))
    request.created_at.CopyFrom(created_at)

    request.source.local_file.path = str(path.resolve())
    request.output_types.extend(normalized_output_types)

    request.options.transcription.model_name = "base"
    request.options.transcription.vad_filter = True
    request.options.diarization.enabled = True

    return request


def submit_job(
    *,
    bootstrap_servers: str,
    topic: str,
    request: JobRequest,
) -> None:
    producer = Producer({"bootstrap.servers": bootstrap_servers})

    def delivery_report(err, msg) -> None:
        if err is not None:
            print(f"Failed to deliver job {request.job_id}: {err}", file=sys.stderr)

    producer.produce(
        topic=topic,
        key=request.job_id.encode("utf-8"),
        value=request.SerializeToString(),
        on_delivery=delivery_report,
    )
    producer.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Submit a Project Vox job to Kafka")
    parser.add_argument(
        "--video-file",
        required=True,
        help="Path to the input video file",
    )
    parser.add_argument(
        "--output-types",
        nargs="+",
        required=True,
        help="Output types to generate: txt json srt vtt",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic",
        default="vox.jobs.request",
        help="Kafka topic for job requests",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    request = build_job_request(
        video_file=args.video_file,
        output_types=args.output_types,
    )

    submit_job(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        request=request,
    )

    print(request.job_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
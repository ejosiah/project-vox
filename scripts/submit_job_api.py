# scripts/submit_job_api.py

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Submit a transcription job via the VOX API.")
    parser.add_argument(
        "--file",
        required=True,
        help="File URL to transcribe.",
    )
    parser.add_argument(
        "--outputs",
        required=True,
        help="Comma-separated output types, e.g. TXT,SRT,JSON",
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:8000/v1/jobs",
        help="VOX API create job endpoint.",
    )
    return parser.parse_args()


def build_payload(file_url: str, outputs: str) -> dict:
    output_types = [item.strip() for item in outputs.split(",") if item.strip()]
    if not output_types:
        raise ValueError("At least one output type is required")

    return {
        "source": {
            "kind": "HTTP_URL",
            "http": {
                "url": file_url,
            },
        },
        "output_types": output_types,
        "options": {
            "transcription": {
                "model_name": "whisper-large-v3",
                "language": "en",
                "vad_filter": True,
            },
            "diarization": {
                "enabled": False,
            },
        },
        "metadata": [
            {
                "key": "source",
                "value": "makefile",
            }
        ],
    }


def submit_job(api_url: str, payload: dict) -> dict:
    request = urllib.request.Request(
        api_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request) as response:
        body = response.read().decode("utf-8")
        return json.loads(body)


def main() -> int:
    args = parse_args()

    try:
        payload = build_payload(args.file, args.outputs)
        response = submit_job(args.api_url, payload)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(f"HTTP {exc.code}: {body}", file=sys.stderr)
        return 1
    except urllib.error.URLError as exc:
        print(f"request failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(response, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
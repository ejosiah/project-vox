# core/pipeline/stages/generate_output_stage.py

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


class GenerateOutputStage(Stage):
    """
    Generates output files for the merged transcript.

    Reads:
    - context.output_types
    - context.metadata["workspace_dir"]
    - context.metadata["merged_transcript"]

    Writes:
    - context.metadata["generated_outputs"]
    """

    name = "generate_output"

    SUPPORTED_OUTPUT_TYPES = {"txt", "json", "srt", "vtt"}

    def run(self, context: JobContext) -> StageResult:
        output_types = list(getattr(context, "output_types", []) or [])
        if not output_types:
            raise ValueError("context.output_types is required for GenerateOutputStage")

        unsupported = sorted(
            output_type
            for output_type in output_types
            if output_type not in self.SUPPORTED_OUTPUT_TYPES
        )
        if unsupported:
            raise ValueError(f"Unsupported output types requested: {', '.join(unsupported)}")

        workspace_dir = context.metadata.get("workspace_dir")
        if not workspace_dir:
            raise ValueError(
                "context.metadata['workspace_dir'] is required for GenerateOutputStage"
            )

        merged_transcript = context.metadata.get("merged_transcript")
        if merged_transcript is None:
            raise ValueError(
                "context.metadata['merged_transcript'] is required for GenerateOutputStage"
            )

        existing_outputs = context.metadata.get("generated_outputs")
        if existing_outputs is not None:
            return StageResult(
                status=StageStatus.SKIPPED,
                context=context,
                metadata={"generated_outputs": existing_outputs},
            )

        output_dir = Path(workspace_dir) / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        generated_outputs: dict[str, str] = {}
        for output_type in output_types:
            output_path = output_dir / f"transcript.{output_type}"
            writer = getattr(self, f"_write_{output_type}")
            writer(output_path, merged_transcript)
            generated_outputs[output_type] = str(output_path)

        context.metadata["generated_outputs"] = generated_outputs

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
            metadata={"generated_outputs": generated_outputs},
        )

    def _write_txt(self, output_path: Path, transcript: Any) -> None:
        lines: list[str] = []
        for segment in getattr(transcript, "segments", []):
            speaker = getattr(segment, "speaker", None) or "UNKNOWN"
            text = str(getattr(segment, "text", "")).strip()
            if not text:
                continue
            lines.append(f"{speaker}: {text}")

        output_path.write_text(
            "\n".join(lines) + ("\n" if lines else ""),
            encoding="utf-8",
        )


    def _write_json(self, output_path: Path, transcript: Any) -> None:
        if hasattr(transcript, "to_dict"):
            payload = transcript.to_dict()
        else:
            payload = {
                "text": getattr(transcript, "text", ""),
                "segments": [
                    {
                        "start": float(getattr(segment, "start", 0.0)),
                        "end": float(getattr(segment, "end", 0.0)),
                        "text": getattr(segment, "text", ""),
                        "speaker": getattr(segment, "speaker", None),
                        "words": list(getattr(segment, "words", [])),
                    }
                    for segment in getattr(transcript, "segments", [])
                ],
            }

        output_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=self._json_default) + "\n",
            encoding="utf-8",
        )

    def _write_srt(self, output_path: Path, transcript: Any) -> None:
        blocks: list[str] = []

        for index, segment in enumerate(getattr(transcript, "segments", []), start=1):
            text = str(getattr(segment, "text", "")).strip()
            if not text:
                continue

            speaker = getattr(segment, "speaker", None) or "UNKNOWN"
            start = self._format_srt_timestamp(float(getattr(segment, "start", 0.0)))
            end = self._format_srt_timestamp(float(getattr(segment, "end", 0.0)))

            blocks.append(f"{index}\n" f"{start} --> {end}\n" f"{speaker}: {text}")

        output_path.write_text(
            "\n\n".join(blocks) + ("\n" if blocks else ""),
            encoding="utf-8",
        )

    def _write_vtt(self, output_path: Path, transcript: Any) -> None:
        blocks = ["WEBVTT"]

        for segment in getattr(transcript, "segments", []):
            text = str(getattr(segment, "text", "")).strip()
            if not text:
                continue

            speaker = getattr(segment, "speaker", None) or "UNKNOWN"
            start = self._format_vtt_timestamp(float(getattr(segment, "start", 0.0)))
            end = self._format_vtt_timestamp(float(getattr(segment, "end", 0.0)))

            blocks.append(f"{start} --> {end}\n{speaker}: {text}")

        output_path.write_text("\n\n".join(blocks) + "\n", encoding="utf-8")

    @staticmethod
    def _format_srt_timestamp(seconds: float) -> str:
        total_milliseconds = int(round(seconds * 1000))
        hours, remainder = divmod(total_milliseconds, 3_600_000)
        minutes, remainder = divmod(remainder, 60_000)
        secs, milliseconds = divmod(remainder, 1000)
        return f"{hours:02d}:{minutes:02d}:{secs:02d},{milliseconds:03d}"

    @staticmethod
    def _format_vtt_timestamp(seconds: float) -> str:
        total_milliseconds = int(round(seconds * 1000))
        hours, remainder = divmod(total_milliseconds, 3_600_000)
        minutes, remainder = divmod(remainder, 60_000)
        secs, milliseconds = divmod(remainder, 1000)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}.{milliseconds:03d}"
    
    @staticmethod
    def _json_default(value: Any) -> Any:
        if hasattr(value, "__dict__"):
            return value.__dict__

        if hasattr(value, "__dataclass_fields__"):
            return {
                field_name: getattr(value, field_name)
                for field_name in value.__dataclass_fields__
            }

        return str(value)

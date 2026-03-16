from __future__ import annotations

from core.media.ffmpeg_utils import FFmpeg
from core.pipeline.context import JobContext
from core.pipeline.stage import Stage, StageResult, StageStatus


class ProbeMediaStage(Stage):
    """
    Probe the downloaded input media and store the probe result on the context.
    """

    name = "probe_media"

    def __init__(self, ffmpeg: FFmpeg | None = None) -> None:
        self._ffmpeg = ffmpeg or FFmpeg()

    def run(self, context: JobContext) -> StageResult:
        if context.input_path is None:
            raise ValueError("context.input_path is required for ProbeMediaStage")

        probe = self._ffmpeg.probe_media(context.input_path)
        context.add_metadata("media_probe", probe)

        return StageResult(
            status=StageStatus.SUCCESS,
            context=context,
            metadata={
                "job_id": context.job_id,
                "input_path": str(context.input_path),
            },
        )
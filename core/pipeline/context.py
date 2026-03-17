from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass(slots=True)
class JobContext:
    """
    Shared job state passed between pipeline stages.

    The context is created by the worker when a Kafka message
    is consumed and then passed through the pipeline where
    stages can read and update it.
    """

    # Core job metadata
    job_id: str
    request: Dict[str, Any]

    # Workspace
    workspace_dir: Optional[Path] = None

    # Input media
    input_uri: Optional[str] = None
    input_path: Optional[Path] = None

    # Audio processing
    audio_path: Optional[Path] = None

    # Transcription output
    transcript: Optional[Any] = None

    # Final outputs
    outputs: Dict[str, Path] = field(default_factory=dict)

    # Arbitrary stage metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    output_types: Optional[list[str]] = None

    # Error tracking
    error: Optional[str] = None

    @classmethod
    def from_kafka_message(cls, message: Dict[str, Any]) -> "JobContext":
        """
        Create a JobContext from a Kafka message payload.
        """
        job_id = message["job_id"]

        return cls(
            job_id=job_id,
            request=message,
            input_uri=message.get("input_uri"),
        )

    def set_workspace(self, path: Path) -> None:
        """Set the workspace directory for the job."""
        self.workspace_dir = path

    def add_output(self, name: str, path: Path) -> None:
        """Register a generated output artifact."""
        self.outputs[name] = path

    def set_error(self, error: Exception | str) -> None:
        """Attach an error to the context."""
        self.error = str(error)

    def add_metadata(self, key: str, value: Any) -> None:
        """Store arbitrary stage metadata."""
        self.metadata[key] = value

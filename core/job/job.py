from pathlib import Path
import uuid


class Job:
    def __init__(self, job_id: str, base_dir: str = "jobs"):
        self.job_id = job_id
        self.base_dir = Path(base_dir)

        self.root = self.base_dir / self.job_id
        self.input_dir = self.root / "input"
        self.audio_dir = self.root / "audio"
        self.output_dir = self.root / "output"
        self.logs_dir = self.root / "logs"

        self._create_workspace()

    @classmethod
    def create(cls, base_dir: str = "jobs"):
        return cls(job_id=uuid.uuid4().hex, base_dir=base_dir)

    def _create_workspace(self):
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self.root.mkdir(exist_ok=True)
        self.input_dir.mkdir(exist_ok=True)
        self.audio_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)

    def paths(self):
        return {
            "job_id": self.job_id,
            "root": str(self.root),
            "input": str(self.input_dir),
            "audio": str(self.audio_dir),
            "output": str(self.output_dir),
            "logs": str(self.logs_dir),
        }

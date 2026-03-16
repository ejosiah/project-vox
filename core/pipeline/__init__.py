"""
Pipeline package.

Provides the core job processing pipeline used by workers to execute
media processing jobs through a sequence of stages.
"""

from .job_runner import JobRunner
from .stage import Stage
from .context import JobContext

__all__ = [
    "JobRunner",
    "Stage",
    "StageResult",
    "JobContext",
]
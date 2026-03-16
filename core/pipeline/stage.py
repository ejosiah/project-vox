from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict


class StageStatus(Enum):
    SUCCESS = auto()
    SKIPPED = auto()
    FAILED = auto()


class StageError(Exception):
    def __init__(self, stage_name: str, message: str, retryable: bool = False):
        super().__init__(message)
        self.stage_name = stage_name
        self.retryable = retryable


@dataclass
class StageResult:
    status: StageStatus
    context: Any
    metadata: Dict[str, Any] = field(default_factory=dict)

class Stage(ABC):
    name: str

    def should_run(self, context: Any) -> bool:
        return True

    @abstractmethod
    def run(self, context: Any) -> StageResult:
        raise NotImplementedError
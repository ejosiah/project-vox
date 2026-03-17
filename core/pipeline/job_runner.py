from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Iterable

from core.pipeline.stage import Stage, StageResult, StageStatus


LOGGER = logging.getLogger(__name__)


class JobRunnerError(Exception):
    """Base exception for job runner failures."""


class StageExecutionError(JobRunnerError):
    """Raised when a stage fails during execution."""

    def __init__(self, stage_name: str, job_id: str | None, cause: Exception):
        self.stage_name = stage_name
        self.job_id = job_id
        self.cause = cause

        message = f"Stage '{stage_name}' failed"
        if job_id is not None:
            message += f" for job '{job_id}'"
        message += f": {cause}"

        super().__init__(message)


class JobRunner:
    """
    Executes a sequence of pipeline stages against a shared job context.
    """

    def __init__(
        self,
        stages: Iterable[Stage],
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._stages = list(stages)
        self._logger = logger or LOGGER

        if not self._stages:
            raise ValueError("JobRunner requires at least one stage")

    def run(self, context: Any) -> Any:
        """
        Run the configured pipeline for the given job context.

        Returns:
            The final updated context.

        Raises:
            StageExecutionError: if any non-cleanup stage fails.
        """
        active_context = context
        failure: StageExecutionError | None = None

        self._logger.info(
            "Starting job pipeline for job_id=%s",
            self._extract_job_id(active_context),
        )

        try:
            for stage in self._non_cleanup_stages:
                active_context = self._run_stage(stage, active_context)
        except StageExecutionError as exc:
            failure = exc
            self._logger.exception(
                "Job pipeline failed for job_id=%s at stage=%s, reasion: %s",
                exc.job_id,
                exc.stage_name,
                active_context.error if hasattr(active_context, "error") else str(exc.cause),
            )
        finally:
            active_context = self._run_cleanup_stages(active_context)

        if failure is not None:
            raise failure

        self._logger.info(
            "Completed job pipeline for job_id=%s",
            self._extract_job_id(active_context),
        )
        return active_context

    @property
    def _non_cleanup_stages(self) -> list[Stage]:
        return [stage for stage in self._stages if not self._is_cleanup_stage(stage)]

    @property
    def _cleanup_stages(self) -> list[Stage]:
        return [stage for stage in self._stages if self._is_cleanup_stage(stage)]

    def _run_stage(self, stage: Stage, context: Any) -> Any:
        stage_name = self._stage_name(stage)
        job_id = self._extract_job_id(context)

        if not self._should_run(stage, context):
            self._logger.info("Skipping stage=%s for job_id=%s", stage_name, job_id)
            return context

        self._logger.info("Running stage=%s for job_id=%s", stage_name, job_id)
        started_at = perf_counter()

        try:
            result = stage.run(context)
            self._logger.debug("Stage=%s for job_id=%s returned result: %s", stage_name, job_id, result)
        except Exception as exc:
            raise StageExecutionError(
                stage_name=stage_name,
                job_id=job_id,
                cause=exc,
            ) from exc

        duration_seconds = perf_counter() - started_at

        if not isinstance(result, StageResult):
            raise StageExecutionError(
                stage_name=stage_name,
                job_id=job_id,
                cause=TypeError(
                    f"Stage '{stage_name}' returned {type(result).__name__}, "
                    "expected StageResult"
                ),
            )

        if result.status == StageStatus.FAILED:
            raise StageExecutionError(
                stage_name=stage_name,
                job_id=self._extract_job_id(result.context),
                cause=RuntimeError("Stage returned FAILED status"),
            )

        if result.status == StageStatus.SKIPPED:
            self._logger.info(
                "Skipped stage=%s for job_id=%s in %.3fs",
                stage_name,
                job_id,
                duration_seconds,
            )
            return context

        next_context = result.context
        if (
            hasattr(next_context, "metadata")
            and isinstance(getattr(next_context, "metadata"), dict)
            and isinstance(result.metadata, dict)
        ):
            next_context.metadata |= result.metadata

        self._logger.info(
            "Completed stage=%s for job_id=%s in %.3fs",
            stage_name,
            self._extract_job_id(next_context),
            duration_seconds,
        )

        return next_context

    def _run_cleanup_stages(self, context: Any) -> Any:
        active_context = context

        for stage in self._cleanup_stages:
            stage_name = self._stage_name(stage)
            job_id = self._extract_job_id(active_context)

            if not self._should_run(stage, active_context):
                self._logger.info(
                    "Skipping cleanup stage=%s for job_id=%s",
                    stage_name,
                    job_id,
                )
                continue

            self._logger.info(
                "Running cleanup stage=%s for job_id=%s",
                stage_name,
                job_id,
            )

            started_at = perf_counter()

            try:
                result = stage.run(active_context)

                if not isinstance(result, StageResult):
                    raise TypeError(
                        f"Cleanup stage '{stage_name}' returned "
                        f"{type(result).__name__}, expected StageResult"
                    )

                if result.status != StageStatus.SKIPPED:
                    active_context = result.context

                if result.status == StageStatus.FAILED:
                    raise RuntimeError("Cleanup stage returned FAILED status")

            except Exception:
                self._logger.exception(
                    "Cleanup stage=%s failed for job_id=%s",
                    stage_name,
                    job_id,
                )
            else:
                duration_seconds = perf_counter() - started_at
                self._logger.info(
                    "Completed cleanup stage=%s for job_id=%s in %.3fs",
                    stage_name,
                    self._extract_job_id(active_context),
                    duration_seconds,
                )

        return active_context

    @staticmethod
    def _should_run(stage: Stage, context: Any) -> bool:
        should_run = getattr(stage, "should_run", None)
        if callable(should_run):
            return bool(should_run(context))
        return True

    @staticmethod
    def _is_cleanup_stage(stage: Stage) -> bool:
        if getattr(stage, "is_cleanup", False):
            return True

        name = getattr(stage, "name", "")
        class_name = stage.__class__.__name__

        return name.lower() == "cleanup" or class_name == "CleanupStage"

    @staticmethod
    def _stage_name(stage: Stage) -> str:
        return getattr(stage, "name", stage.__class__.__name__)

    @staticmethod
    def _extract_job_id(context: Any) -> str | None:
        if context is None:
            return None

        if hasattr(context, "job_id"):
            value = getattr(context, "job_id")
            return None if value is None else str(value)

        if isinstance(context, dict):
            value = context.get("job_id")
            return None if value is None else str(value)

        return None
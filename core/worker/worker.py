from __future__ import annotations

import logging
from typing import Any, Callable

from core.pipeline.context import JobContext
from core.pipeline.job_runner import JobRunner


LOGGER = logging.getLogger(__name__)


class Worker:
    """
    Consumes JobRequest messages from Kafka, executes the pipeline,
    and produces JobResult messages.
    """

    def __init__(
        self,
        *,
        consumer: Any,
        producer: Any,
        job_runner: JobRunner,
        request_deserializer: Callable[[bytes], Any],
        result_serializer: Callable[[Any], bytes],
        input_topic: str,
        output_topic: str,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._job_runner = job_runner
        self._request_deserializer = request_deserializer
        self._result_serializer = result_serializer
        self._input_topic = input_topic
        self._output_topic = output_topic

    def run_once(self) -> None:
        """
        Poll once and process available messages.
        """
        records = self._consumer.poll(timeout_ms=1000)

        for _, messages in records.items():
            for message in messages:
                self._handle_message(message)
                self._consumer.commit()

    def _handle_message(self, message: Any) -> None:
        try:
            request = self._request_deserializer(message.value)
            context = self._build_context(request)

            LOGGER.info("Processing job_id=%s", context.job_id)

            result = self._job_runner.run(context)

            result_message = self._build_result_message(context, result)
            payload = self._result_serializer(result_message)

            self._producer.send(self._output_topic, payload)
            self._producer.flush()

        except Exception as exc:
            LOGGER.exception("Failed processing message: %s", exc)

    def _build_context(self, request: Any) -> JobContext:
        job_id = getattr(request, "job_id")

        output_types = [self._map_output_type(o) for o in getattr(request, "output_types", [])]

        context = JobContext(
            job_id=job_id,
            request=request,
            metadata={},
        )

        context.output_types = output_types

        return context

    def _build_result_message(self, context: JobContext, result: Any) -> dict[str, Any]:
        outcome = context.metadata.get("job_outcome", {})

        return {
            "job_id": context.job_id,
            "status": outcome.get("status", "unknown"),
            "outputs": self._serialize_outputs(context.metadata.get("uploaded_outputs")),
        }

    @staticmethod
    def _serialize_outputs(uploaded_outputs: Any) -> dict[str, Any]:
        if not uploaded_outputs or not hasattr(uploaded_outputs, "outputs"):
            return {}

        serialized: dict[str, Any] = {}

        for key, value in uploaded_outputs.outputs.items():
            serialized[key] = {
                "destination": getattr(value, "destination", None),
                "download_url": getattr(value, "download_url", None),
            }

        return serialized

    @staticmethod
    def _map_output_type(output_type: Any) -> str:
        # protobuf enum → string
        if hasattr(output_type, "name"):
            return output_type.name.replace("OUTPUT_TYPE_", "").lower()
        return str(output_type)
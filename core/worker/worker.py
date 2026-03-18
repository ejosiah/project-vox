# core/worker/worker.py

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from core.worker.message_mapper import MessageMapper


LOGGER = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        *,
        consumer: Any,
        producer: Any,
        job_runner: Any,
        request_deserializer: Any,
        result_serializer: Any,
        input_topic: str,
        output_topic: str,
        message_mapper: MessageMapper | None = None,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._job_runner = job_runner
        self._request_deserializer = request_deserializer
        self._result_serializer = result_serializer
        self._input_topic = input_topic
        self._output_topic = output_topic
        self._message_mapper = message_mapper or MessageMapper()

    def run_once(self) -> None:
        records = self._consumer.poll(timeout_ms=1000)

        for _, messages in records.items():
            for message in messages:
                self._handle_message(message)

    def _handle_message(self, message: Any) -> None:
        request = self._request_deserializer(message.value)
        context = self._message_mapper.request_to_context(request)
        context.metadata["started_at"] = datetime.now(timezone.utc).isoformat()

        try:
            self._job_runner.run(context)
            result_message = self._message_mapper.context_to_result(
                context=context,
                success=True,
            )
        except Exception as exc:
            LOGGER.exception("Failed processing job_id=%s", context.job_id)
            result_message = self._message_mapper.context_to_result(
                context=context,
                success=False,
                error=exc,
            )

        payload = self._result_serializer(result_message)
        self._producer.send(self._output_topic, payload)
        self._producer.flush()
        self._consumer.commit()
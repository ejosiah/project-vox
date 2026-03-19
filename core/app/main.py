# app/main.py

from __future__ import annotations

import logging
import json
from dataclasses import asdict, is_dataclass

from core.app.bootstrap import build_worker, configure_logging
from core.app.settings import load_settings


LOGGER = logging.getLogger(__name__)


def _serialize_settings(settings) -> dict:
    """
    Safely convert settings to a loggable dict.
    Handles dataclasses and avoids non-serializable fields.
    """
    if is_dataclass(settings):
        data = asdict(settings)
    else:
        data = settings.__dict__.copy()

    # redact anything sensitive if needed later
    return data


def main() -> int:
    settings = load_settings()
    configure_logging(settings.log_level)

    # Log settings (pretty + safe)
    try:
        settings_dict = _serialize_settings(settings)
        LOGGER.info(
            "Application settings:\n%s",
            json.dumps(settings_dict, indent=2, default=str),
        )
    except Exception as exc:
        LOGGER.warning("Failed to serialize settings for logging: %s", exc)

    worker = build_worker(settings)

    LOGGER.info("Worker started, entering processing loop...")

    while True:
        worker.run_once()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
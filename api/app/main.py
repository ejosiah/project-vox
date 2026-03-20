# api/app/main.py

from __future__ import annotations

import json
import logging

from dataclasses import asdict, is_dataclass
from typing import Any

import uvicorn

from api.app.bootstrap import build_app, configure_logging
from api.app.settings import load_settings

LOGGER = logging.getLogger(__name__)


def _serialize_settings(settings: Any) -> dict[str, Any]:
    if is_dataclass(settings):
        return asdict(settings)

    if hasattr(settings, "__dict__"):
        return dict(settings.__dict__)

    return {"value": str(settings)}


def main() -> int:
    settings = load_settings()
    configure_logging(settings.api.log_level)

    LOGGER.info(
        "Loaded API settings: %s",
        json.dumps(_serialize_settings(settings), sort_keys=True),
    )

    app = build_app(settings)

    uvicorn.run(
        app,
        host=settings.api.host,
        port=settings.api.port,
        log_level=settings.api.log_level.lower(),
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
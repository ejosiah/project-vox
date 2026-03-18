# app/main.py

from __future__ import annotations

from app.bootstrap import build_worker, configure_logging
from app.settings import load_settings


def main() -> int:
    settings = load_settings()
    configure_logging(settings.log_level)

    worker = build_worker(settings)

    while True:
        worker.run_once()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
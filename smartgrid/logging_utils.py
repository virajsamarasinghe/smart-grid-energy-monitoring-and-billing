"""Structured (JSON-lines) logging used by every pipeline stage.

Usage::

    log = get_logger("simulator")
    log.info("reading_sent", household_id="H001", partition=2)

Each line is a JSON object with ``ts``, ``level``, ``service``, ``event`` and any extra fields,
so logs from all containers can be grepped or loaded into a log store consistently.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str):
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "service": self.service,
            "event": record.getMessage(),
        }
        payload.update(getattr(record, "fields", {}) or {})
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class StructLogger:
    def __init__(self, logger: logging.Logger):
        self._logger = logger

    def _log(self, level: int, event: str, exc_info=False, **fields) -> None:
        self._logger.log(level, event, exc_info=exc_info, extra={"fields": fields})

    def debug(self, event: str, **fields) -> None:
        self._log(logging.DEBUG, event, **fields)

    def info(self, event: str, **fields) -> None:
        self._log(logging.INFO, event, **fields)

    def warning(self, event: str, **fields) -> None:
        self._log(logging.WARNING, event, **fields)

    def error(self, event: str, **fields) -> None:
        self._log(logging.ERROR, event, **fields)

    def exception(self, event: str, **fields) -> None:
        self._log(logging.ERROR, event, exc_info=True, **fields)


def get_logger(service: str) -> StructLogger:
    logger = logging.getLogger(f"smartgrid.{service}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter(service))
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
        logger.propagate = False
    return StructLogger(logger)

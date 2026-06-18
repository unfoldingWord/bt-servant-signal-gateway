"""Structured JSON logging.

Mirrors the intent of the sibling gateways' loggers (one structured line per
event, secrets redacted) on the stdlib ``logging`` module — no extra dependency.
``configure_logging`` installs a single stdout handler that emits one JSON
object per log record; any ``extra`` fields whose name looks secret are masked.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

_REDACTED = "***redacted***"

# Reserved attributes on a LogRecord that are not user-supplied "extra" fields.
_RESERVED = set(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime", "taskName"}

# Field names that should never have their value logged in the clear.
_SECRET_HINTS = ("token", "key", "secret", "password", "authorization")


def _is_secret(name: str) -> bool:
    lowered = name.lower()
    return any(hint in lowered for hint in _SECRET_HINTS)


class JsonFormatter(logging.Formatter):
    """Render a log record as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for name, value in record.__dict__.items():
            if name in _RESERVED:
                continue
            payload[name] = _REDACTED if _is_secret(name) else value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    """Install the JSON formatter on the root logger (idempotent)."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

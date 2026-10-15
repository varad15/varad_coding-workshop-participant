"""
Structured logging.

Every log line is a single JSON object, e.g.:

    {"ts": "2026-10-15T02:30:01Z", "level": "INFO", "logger": "bronze",
     "event": "step_finished", "run_id": "...", "step": "bronze.employees",
     "rows_read": 2000, "rows_written": 2000, "duration_s": 1.2}

JSON lines are easy to read in a terminal and easy to search with
`kubectl logs ... | grep` or CloudWatch Insights.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

_RESERVED = set(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}
_context: dict[str, Any] = {}


class JsonFormatter(logging.Formatter):
    """Render log records as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a record, merging run context and `extra` fields."""
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        payload.update(_context)
        for key, value in vars(record).items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    """Configure the root logger once for the whole process."""
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level)
    # Third-party libraries are noisy at INFO.
    for noisy in ("py4j", "botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def bind_context(**fields: Any) -> None:
    """Attach fields (e.g. run_id) to every subsequent log line."""
    _context.update(fields)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger."""
    return logging.getLogger(name)

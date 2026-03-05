"""
logging_config.py – Structured JSON logging setup for all pipeline services.

Provides a single ``setup_logging()`` function that configures the root logger
to emit JSON-formatted log lines using ``python-json-logger``.  This makes logs
machine-parseable for ELK/Loki/CloudWatch ingestion while still being human-
readable in docker compose logs.

Usage
-----
Call once at service startup, before any other logging:

>>> from services.common.logging_config import setup_logging
>>> setup_logging()               # uses settings.log_level
>>> setup_logging("DEBUG")        # override level
"""

import logging
import sys

from pythonjsonlogger.json import JsonFormatter

from services.common.config import settings


def setup_logging(level: str | None = None) -> None:
    """
    Configure the root logger for structured JSON output to stderr.

    Parameters
    ----------
    level : str, optional
        Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        Defaults to ``settings.log_level`` if not specified.
    """
    log_level = (level or settings.log_level).upper()

    formatter = JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={
            "asctime": "timestamp",
            "levelname": "level",
            "name": "logger",
        },
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level)

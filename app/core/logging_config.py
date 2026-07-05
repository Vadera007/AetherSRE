"""
AetherSRE — Structured Logging Configuration
=============================================
Configures the Python standard `logging` module with a clean, structured
formatter suitable for both human readability and future JSON log shipping.
"""

from __future__ import annotations

import logging
import sys
from typing import Any


class AetherFormatter(logging.Formatter):
    """
    Custom log formatter that emits fixed-width, coloured output on TTYs
    and plain structured text in non-interactive / container environments.

    Format:
        2024-01-15 12:34:56,789 | INFO     | aether.core.config     | message here
    """

    # ANSI colour codes (disabled when stdout is not a TTY)
    _COLOURS: dict[int, str] = {
        logging.DEBUG: "\033[36m",      # Cyan
        logging.INFO: "\033[32m",       # Green
        logging.WARNING: "\033[33m",    # Yellow
        logging.ERROR: "\033[31m",      # Red
        logging.CRITICAL: "\033[35m",   # Magenta
    }
    _RESET = "\033[0m"

    def __init__(self, use_colour: bool | None = None) -> None:
        super().__init__()
        self._use_colour: bool = (
            sys.stdout.isatty() if use_colour is None else use_colour
        )

    def format(self, record: logging.LogRecord) -> str:  # noqa: ANN001
        ts = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        level = record.levelname.ljust(8)
        name = record.name[:30].ljust(30)
        message = record.getMessage()

        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)

        if self._use_colour:
            colour = self._COLOURS.get(record.levelno, "")
            level = f"{colour}{level}{self._RESET}"

        return f"{ts} | {level} | {name} | {message}"


def configure_logging(level: str = "INFO") -> None:
    """
    Apply AetherSRE's logging configuration globally.

    This must be called once at application startup, before any loggers
    are created, to ensure consistent formatting across all modules.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(AetherFormatter())
    handler.setLevel(numeric_level)

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    # Remove any handlers added by third-party libraries before ours
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    # Silence noisy third-party loggers
    for noisy in ("uvicorn.access", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logging.getLogger("aether").info(
        "Logging initialised | level=%s | formatter=AetherFormatter", level
    )


def get_logger(name: str) -> logging.Logger:
    """
    Retrieve a namespaced logger.

    All internal loggers are children of the 'aether' hierarchy so that
    a single `logging.getLogger('aether').setLevel(...)` can silence all
    internal noise during tests.

    Args:
        name: Typically passed as __name__ from the calling module.

    Returns:
        A configured Logger instance.
    """
    # Normalise: strip top-level 'app.' prefix and replace with 'aether.'
    canonical = name.replace("app.", "aether.", 1) if name.startswith("app.") else name
    return logging.getLogger(canonical)

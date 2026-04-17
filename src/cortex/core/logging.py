"""Logging utilities for Cortex.

Usage:
    from cortex.core.logging import configure_logging, get_logger
    configure_logging("INFO")
    logger = get_logger("cortex.journal")
"""
import logging


def configure_logging(level: str = "INFO") -> None:
    """Configure root logger with a standard format.

    Safe to call multiple times — each call updates the root logger level and
    adds a handler only if none are present (basicConfig semantics).
    """
    logging.basicConfig(
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    # Set the level explicitly so repeated calls work correctly
    logging.root.setLevel(level)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger."""
    return logging.getLogger(name)

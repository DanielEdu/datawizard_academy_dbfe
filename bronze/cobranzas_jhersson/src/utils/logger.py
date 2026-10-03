"""Logging estándar del job."""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Logger con formato único hacia stdout (lo muestra el output del job)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger

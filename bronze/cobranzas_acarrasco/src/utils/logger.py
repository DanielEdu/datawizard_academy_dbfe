"""Logging estándar del job."""

import logging
import sys

_FORMAT = "%(asctime)s %(levelname)s %(name)s - %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Devuelve un logger que escribe a stdout (visible en el output del job)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger

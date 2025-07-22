# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
import os

DEFAULT_LOG_LEVEL = logging.INFO


def _get_numeric_log_level(level_str: str) -> int:
    """Convert a string log level to its numeric equivalent.

    Args:
        level_str: The log level as a string (e.g., "INFO", "DEBUG")

    Returns:
        The numeric log level, or DEFAULT_LOG_LEVEL if invalid
    """
    numeric_level = getattr(logging, level_str.upper(), None)
    if not isinstance(numeric_level, int):
        return DEFAULT_LOG_LEVEL
    return numeric_level


def _get_default_log_level() -> int:
    """Get the default log level from environment or return INFO."""
    if level_str := os.getenv("TDP_LOG_LEVEL", "").strip().upper():
        return _get_numeric_log_level(level_str)
    return DEFAULT_LOG_LEVEL


def setup_early_logging() -> None:
    """
    Set up early logging configuration before CLI parsing.
    This ensures logging is available for early callbacks.
    """
    # Only configure if not already configured
    if logging.getLogger().handlers:
        return

    log_level = _get_default_log_level()

    # Create a console handler with the default log level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    # Create a formatter and attach it to the handler
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    # Get the root logger and set its level
    logger = logging.getLogger()
    logger.setLevel(log_level)
    # Add the console handler to the logger
    logger.addHandler(console_handler)


def setup_logging(log_level: str) -> None:
    """
    Reconfigure the logging module with user-specified level.

    Parameters:
        log_level: The desired logging level as a string (e.g., "info").
    """
    # Ensure the provided log level is valid, using the default if it is not.
    numeric_level = _get_numeric_log_level(log_level)

    # Update existing logger configuration
    logger = logging.getLogger()
    logger.setLevel(numeric_level)

    # Update all existing handlers
    for handler in logger.handlers:
        handler.setLevel(numeric_level)


def get_early_logger(name: str) -> logging.Logger:
    """
    Get a logger that's guaranteed to work even before CLI parsing.

    Parameters:
        name: The logger name (usually __name__)

    Returns:
        A configured logger instance.
    """
    setup_early_logging()
    return logging.getLogger(name)

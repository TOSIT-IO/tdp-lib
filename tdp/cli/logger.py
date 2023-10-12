# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging

DEFAULT_LOG_LEVEL = logging.INFO


def setup_logging(log_level: str) -> None:
    """
    Configure the logging module.

    Parameters:
        log_level: The desired logging level as a string (e.g., "info").
    """
    # Ensure the provided log level is valid, using the default if it is not.
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        print(f"Invalid log level: {log_level}. Using {DEFAULT_LOG_LEVEL} instead.")
        numeric_level = DEFAULT_LOG_LEVEL

    # Create a console handler with the specified log level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    # Create a formatter and attach it to the handler
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    # Get the root logger and set its level to the specified level
    logger = logging.getLogger()
    logger.setLevel(numeric_level)
    # Add the console handler to the logger
    logger.addHandler(console_handler)

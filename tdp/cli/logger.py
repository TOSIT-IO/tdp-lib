# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging.config

DEFAULT_LOG_LEVEL = logging.INFO


def setup_logging(log_conf_file: str) -> None:
    """
    Configure the logging module.

    Parameters:
        log_conf_file: The conf file for the logging.
    """
    if log_conf_file:
        logging.config.fileConfig(log_conf_file)
    else:
        # Default behaviour : using the default log level.
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

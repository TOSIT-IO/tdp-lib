# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging

# create logger
logger = logging.getLogger("tdp")
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s.%(funcName)s - %(message)s"
)

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

# 'application' code
logger.debug("Logger initialized.")

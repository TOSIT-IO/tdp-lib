# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

"""This module defines project-level constants."""

# Directory names
DAG_DIRECTORY_NAME = "tdp_lib_dag"
DEFAULT_VARS_DIRECTORY_NAME = "tdp_vars_defaults"
PLAYBOOKS_DIRECTORY_NAME = "playbooks"
SCHEMA_VARS_DIRECTORY_NAME = "tdp_vars_schema"
# File extensions
JSON_EXTENSION = ".json"
YML_EXTENSION = ".yml"
# Special operations
OPERATION_SLEEP_NAME = "wait_sleep"
OPERATION_SLEEP_VARIABLE = "wait_sleep_seconds"
# Max lengths
HOST_NAME_MAX_LENGTH = 255
VERSION_MAX_LENGTH = 40
SERVICE_NAME_MAX_LENGTH = 20
COMPONENT_NAME_MAX_LENGTH = 30
ACTION_NAME_MAX_LENGTH = 20
OPERATION_NAME_MAX_LENGTH = (
    SERVICE_NAME_MAX_LENGTH + COMPONENT_NAME_MAX_LENGTH + ACTION_NAME_MAX_LENGTH
)
# Service priority
SERVICE_PRIORITY = {
    "exporter": 1,
    "zookeeper": 2,
    "hadoop": 3,
    "ranger": 4,
    "hdfs": 5,
    "yarn": 6,
    "hive": 7,
    "hbase": 8,
    "spark": 9,
    "spark3": 10,
    "knox": 11,
}
DEFAULT_SERVICE_PRIORITY = 99

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Generator
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from tdp.core.constants import (
    YML_EXTENSION,
)

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

logger = logging.getLogger(__name__)


def read_dag_directory(
    directory_path: Path,
) -> Generator[TDPLibDagNodeModel, None, None]:
    """Get the DAG nodes of a collection.

    Args:
        directory_path: Path to the DAG directory.

    Returns:
        List of DAG nodes.
    """
    for dag_file in (directory_path).glob("*" + YML_EXTENSION):
        yield from read_dag_file(dag_file)


class TDPLibDagNodeModel(BaseModel):
    """Model for a TDP operation defined in a tdp_lib_dag file."""

    model_config = ConfigDict(extra="ignore")

    name: str
    depends_on: list[str] = []


class TDPLibDagModel(BaseModel):
    """Model for a TDP DAG defined in a tdp_lib_dag file."""

    model_config = ConfigDict(extra="ignore")

    operations: list[TDPLibDagNodeModel]


def read_dag_file(
    file_path: Path,
) -> Generator[TDPLibDagNodeModel, None, None]:
    """Read a tdp_lib_dag file and return a list of DAG operations.

    Args:
        file_path: Path to the tdp_lib_dag file.
    """
    with file_path.open("r") as operations_file:
        file_content = yaml.load(operations_file, Loader=Loader)

    try:
        tdp_lib_dag = TDPLibDagModel(operations=file_content)
        for operation in tdp_lib_dag.operations:
            yield operation
    except ValidationError as e:
        logger.error(f"Error while parsing tdp_lib_dag file {file_path}: {e}")
        raise

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Generator
from pathlib import Path
from typing import NamedTuple

import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine

from tdp.cli.commands.init import init
from tdp.core.models.base_model import BaseModel
from tests.conftest import generate_collection_at_path


class TDPInitArgs(NamedTuple):
    collection_path: Path
    db_dsn: str
    vars: Path


@pytest.fixture
def tdp_init(
    collection_path: Path, db_dsn: str, vars: Path
) -> Generator[TDPInitArgs, None, None]:
    base_args = [
        "--collection-path",
        str(collection_path),
        "--database-dsn",
        db_dsn,
        "--vars",
        str(vars),
    ]
    runner = CliRunner()
    runner.invoke(init, base_args)
    yield TDPInitArgs(collection_path, db_dsn, vars)
    engine = create_engine(db_dsn)
    BaseModel.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture
def collection_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    collection_path = tmp_path_factory.mktemp("collection")
    dag_service_operations = {
        "service": [
            {"name": "service_install"},
            {"name": "service_config", "depends_on": ["service_install"]},
            {"name": "service_start", "depends_on": ["service_config"]},
            {"name": "service_init", "depends_on": ["service_start"]},
        ],
    }
    service_vars = {
        "service": {
            "service": {},
        },
    }
    generate_collection_at_path(collection_path, dag_service_operations, service_vars)
    return collection_path


@pytest.fixture
def vars(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("collection")

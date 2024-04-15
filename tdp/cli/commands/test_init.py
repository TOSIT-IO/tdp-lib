# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.init import init


def test_tdp_init(collection_path: Path, database_dsn_path: str, vars: Path):
    args = [
        "--collection-path",
        collection_path,
        "--database-dsn",
        database_dsn_path,
        "--vars",
        vars,
    ]
    runner = CliRunner()
    result = runner.invoke(init, args)
    assert result.exit_code == 0, result.output


def test_tdp_init_db_is_created(collection_path: Path, vars: Path, tmp_path: Path):
    db_path = tmp_path / "sqlite.db"
    args = [
        "--collection-path",
        collection_path,
        "--database-dsn",
        "sqlite:///" + str(db_path),
        "--vars",
        vars,
    ]
    runner = CliRunner()
    result = runner.invoke(init, args)
    assert os.path.exists(db_path) == True
    assert result.exit_code == 0, result.output

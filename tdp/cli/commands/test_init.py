# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os

from click.testing import CliRunner

from tdp.cli.commands.init import init


def test_tdp_init(collection_path, database_dsn, vars):
    args = [
        "--collection-path",
        collection_path,
        "--database-dsn",
        database_dsn,
        "--vars",
        vars,
    ]
    runner = CliRunner()
    result = runner.invoke(init, args)
    assert result.exit_code == 0


def test_tdp_init_db_is_created(collection_path, vars, tmp_path):
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
    assert result.exit_code == 0

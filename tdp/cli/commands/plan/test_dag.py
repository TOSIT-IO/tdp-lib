# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from click.testing import CliRunner

from tdp.cli.commands.init import init
from tdp.cli.commands.plan.dag import dag


def test_tdp_plan_dag(collection_path, database_dsn_path, vars):
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
    assert result.exit_code == 0
    result = runner.invoke(dag, args)
    assert result.exit_code == 0

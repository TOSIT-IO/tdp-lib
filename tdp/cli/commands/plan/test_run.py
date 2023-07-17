# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from click.testing import CliRunner
from .run import run
from ..init import init


def test_tdp_plan_run(collection_path, database_dsn_path, vars):
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
    args.append("service_install")
    result = runner.invoke(run, args)
    assert result.exit_code == 0

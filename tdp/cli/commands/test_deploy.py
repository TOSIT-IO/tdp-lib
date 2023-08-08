# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from click.testing import CliRunner

from tdp.cli.commands.deploy import deploy
from tdp.cli.commands.init import init
from tdp.cli.commands.plan.dag import dag


def test_tdp_deploy_mock(collection_path, database_dsn_path, vars, tmp_path):
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
    args.extend(["--run-directory", tmp_path, "--mock-deploy"])
    result = runner.invoke(deploy, args)
    assert result.exit_code == 0

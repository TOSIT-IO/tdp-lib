# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.deploy import deploy
from tdp.cli.commands.init import init
from tdp.cli.commands.plan.dag import dag


def test_tdp_deploy_mock(
    collection_path: Path, database_dsn_path: str, vars: Path, tmp_path: Path
):
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
    result = runner.invoke(dag, args)
    assert result.exit_code == 0, result.output
    args.extend(["--run-directory", tmp_path, "--mock-deploy"])
    result = runner.invoke(deploy, args)
    assert result.exit_code == 0, result.output

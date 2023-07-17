# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from click.testing import CliRunner
from .plan.dag import dag
from .init import init
from .deploy import deploy


def test_tdp_deploy_mock(collection_path, database_dsn_path, vars):
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
    args.append("--mock-deploy")
    result = runner.invoke(deploy, args)
    assert result.exit_code == 0

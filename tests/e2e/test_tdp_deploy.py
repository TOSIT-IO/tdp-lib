# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.deploy import deploy
from tdp.cli.commands.plan.dag import dag
from tests.e2e.conftest import TDPInitArgs


def test_tdp_deploy_mock(
    tdp_init: TDPInitArgs,
    tmp_path: Path,
):
    runner = CliRunner()
    result = runner.invoke(
        dag,
        [
            "--collection-path",
            str(tdp_init.collection_path),
            "--database-dsn",
            tdp_init.db_dsn,
        ],
    )
    assert result.exit_code == 0, result.output
    result = runner.invoke(
        deploy,
        [
            "--collection-path",
            str(tdp_init.collection_path),
            "--database-dsn",
            tdp_init.db_dsn,
            "--vars",
            str(tdp_init.vars),
            "--run-directory",
            str(tmp_path),
            "--mock-deploy",
        ],
    )
    assert result.exit_code == 0, result.output

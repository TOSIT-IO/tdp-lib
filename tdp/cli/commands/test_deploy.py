# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.conftest import tdp_init_args
from tdp.cli.commands.deploy import deploy
from tdp.cli.commands.plan.dag import dag


def test_tdp_deploy_mock(
    tdp_init: tdp_init_args,
    tmp_path: Path,
):
    runner = CliRunner()
    result = runner.invoke(
        dag,
        [
            "--collection-path",
            tdp_init.collection_path,
            "--database-dsn",
            tdp_init.db_dsn,
        ],
    )
    assert result.exit_code == 0, result.output
    result = runner.invoke(
        deploy,
        [
            *[
                "--collection-path",
                tdp_init.collection_path,
                "--database-dsn",
                tdp_init.db_dsn,
                "--vars",
                tdp_init.vars,
            ],
            "--run-directory",
            str(tmp_path),
            "--mock-deploy",
        ],
    )
    assert result.exit_code == 0, result.output

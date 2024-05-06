# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.deploy import deploy
from tdp.cli.commands.plan.dag import dag


def test_tdp_deploy_mock(
    tdp_init: list,
    tmp_path: Path,
):
    runner = CliRunner()
    result = runner.invoke(dag, tdp_init[:-2])
    assert result.exit_code == 0, result.output
    result = runner.invoke(
        deploy,
        [
            *tdp_init,
            "--run-directory",
            str(tmp_path),
            "--mock-deploy",
        ],
    )
    assert result.exit_code == 0, result.output

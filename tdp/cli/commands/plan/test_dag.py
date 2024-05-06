# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.plan.dag import dag


def test_tdp_plan_dag(
    tdp_init: list,
):
    runner = CliRunner()
    result = runner.invoke(dag, tdp_init[:-2])
    assert result.exit_code == 0, result.output

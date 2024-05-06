# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.plan.ops import ops


def test_tdp_plan_run(
    tdp_init: list,
):
    runner = CliRunner()
    result = runner.invoke(ops, [*tdp_init[:-2], "service_install"])
    assert result.exit_code == 0, result.output

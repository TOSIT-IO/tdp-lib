# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.plan.ops import ops


def test_tdp_plan_run(
    tdp_init: tuple,
):
    tdp_init_args = [
        "--collection-path",
        tdp_init[0],
        "--database-dsn",
        tdp_init[1],
    ]
    runner = CliRunner()
    result = runner.invoke(ops, [*tdp_init_args, "service_install"])
    assert result.exit_code == 0, result.output

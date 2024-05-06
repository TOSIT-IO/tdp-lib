# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.plan.reconfigure import reconfigure


def test_tdp_plan_reconfigure(
    tdp_init: tuple,
):
    tdp_init_args = [
        "--collection-path",
        tdp_init[0],
        "--database-dsn",
        tdp_init[1],
    ]
    runner = CliRunner()
    result = runner.invoke(reconfigure, tdp_init_args)
    assert (
        result.exit_code == 1
    ), result.output  # No stale components, hence nothing to reconfigure.

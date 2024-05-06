# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.conftest import TDPInitArgs
from tdp.cli.commands.plan.reconfigure import reconfigure


def test_tdp_plan_reconfigure(
    tdp_init: TDPInitArgs,
):
    runner = CliRunner()
    result = runner.invoke(
        reconfigure,
        [
            "--collection-path",
            tdp_init.collection_path,
            "--database-dsn",
            tdp_init.db_dsn,
        ],
    )
    assert (
        result.exit_code == 1
    ), result.output  # No stale components, hence nothing to reconfigure.

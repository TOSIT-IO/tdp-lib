# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.conftest import tdp_init_args
from tdp.cli.commands.plan.reconfigure import reconfigure


def test_tdp_plan_reconfigure(
    tdp_init: tdp_init_args,
):
    base_args = [
        "--collection-path",
        tdp_init.collection_path,
        "--database-dsn",
        tdp_init.db_dsn,
    ]
    runner = CliRunner()
    result = runner.invoke(reconfigure, base_args)
    assert (
        result.exit_code == 1
    ), result.output  # No stale components, hence nothing to reconfigure.

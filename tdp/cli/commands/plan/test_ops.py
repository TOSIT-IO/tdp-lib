# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.conftest import tdp_init_args
from tdp.cli.commands.plan.ops import ops


def test_tdp_plan_run(
    tdp_init: tdp_init_args,
):
    runner = CliRunner()
    result = runner.invoke(
        ops,
        [
            *[
                "--collection-path",
                tdp_init.collection_path,
                "--database-dsn",
                tdp_init.db_dsn,
            ],
            "service_install",
        ],
    )
    assert result.exit_code == 0, result.output

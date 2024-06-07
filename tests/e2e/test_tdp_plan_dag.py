# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.plan.dag import dag
from tests.e2e.conftest import TDPInitArgs


def test_tdp_plan_dag(
    tdp_init: TDPInitArgs,
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

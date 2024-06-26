# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.status.prune_hosts import prune_hosts
from tests.e2e.conftest import TDPInitArgs


def test_tdp_status_edit(
    tdp_init: TDPInitArgs,
):
    runner = CliRunner()
    result = runner.invoke(
        prune_hosts,
        [
            "--database-dsn",
            tdp_init.db_dsn,
        ],
    )
    assert result.exit_code == 0, result.output

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.conftest import TDPInitArgs
from tdp.cli.commands.status.edit import edit


def test_tdp_status_edit(
    tdp_init: TDPInitArgs,
):
    runner = CliRunner()
    result = runner.invoke(
        edit,
        [
            "--collection-path",
            tdp_init.collection_path,
            "--database-dsn",
            tdp_init.db_dsn,
            "--vars",
            tdp_init.vars,
            "service",
            "--host",
            "localhost",
        ],
    )
    assert result.exit_code == 0, result.output

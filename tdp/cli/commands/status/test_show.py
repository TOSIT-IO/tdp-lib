# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.conftest import tdp_init_args
from tdp.cli.commands.status.show import show


def test_tdp_status_edit(
    tdp_init: tdp_init_args,
):
    runner = CliRunner()
    result = runner.invoke(
        show,
        [
            "--collection-path",
            tdp_init.collection_path,
            "--database-dsn",
            tdp_init.db_dsn,
            "--vars",
            tdp_init.vars,
        ],
    )
    assert result.exit_code == 0, result.output

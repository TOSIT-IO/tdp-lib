# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.status.edit import edit


def test_tdp_status_edit(
    tdp_init: tuple,
):
    tdp_init_args = [
        "--collection-path",
        tdp_init[0],
        "--database-dsn",
        tdp_init[1],
        "--vars",
        tdp_init[2],
    ]
    runner = CliRunner()
    result = runner.invoke(edit, [*tdp_init_args, "service", "--host", "localhost"])
    assert result.exit_code == 0, result.output

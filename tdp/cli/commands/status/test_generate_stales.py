# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.status.generate_stales import generate_stales


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
    result = runner.invoke(generate_stales, tdp_init_args)
    assert result.exit_code == 0, result.output

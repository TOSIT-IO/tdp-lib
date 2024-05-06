# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.status.show import show


def test_tdp_status_edit(
    tdp_init: list,
):
    runner = CliRunner()
    result = runner.invoke(show, tdp_init)
    assert result.exit_code == 0, result.output

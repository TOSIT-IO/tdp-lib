# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.conftest import tdp_init_args
from tdp.cli.commands.status.generate_stales import generate_stales


def test_tdp_status_edit(
    tdp_init: tdp_init_args,
):
    base_args = [
        "--collection-path",
        tdp_init.collection_path,
        "--database-dsn",
        tdp_init.db_dsn,
        "--vars",
        tdp_init.vars,
    ]
    runner = CliRunner()
    result = runner.invoke(generate_stales, base_args)
    assert result.exit_code == 0, result.output

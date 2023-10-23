# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.init import init
from tdp.cli.commands.status.generate_stales import generate_stales


def test_tdp_status_edit(collection_path: Path, database_dsn_path: str, vars: Path):
    base_args = [
        "--collection",
        collection_path,
        "--database-dsn",
        database_dsn_path,
        "--vars",
        str(vars),
    ]
    runner = CliRunner()
    result = runner.invoke(init, base_args)
    assert result.exit_code == 0, result.output
    result = runner.invoke(generate_stales, base_args)
    assert result.exit_code == 0, result.output

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.init import init
from tdp.cli.commands.status import status


def test_tdp_status(collection_path: Path, database_dsn_path: str, vars: Path):
    base_args = [
        "--collection-path",
        collection_path,
        "--database-dsn",
        database_dsn_path,
    ]
    runner = CliRunner()
    result = runner.invoke(init, [*base_args, "--vars", str(vars)])
    assert result.exit_code == 0, result.output
    args = [
        *base_args,
        "--vars",
        str(vars),
    ]
    runner = CliRunner()
    result = runner.invoke(status, args)
    assert result.exit_code == 0, result.output

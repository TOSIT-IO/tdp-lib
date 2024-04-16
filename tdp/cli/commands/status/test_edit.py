# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.init import init
from tdp.cli.commands.status.edit import edit


def test_tdp_status_edit(collection_path: Path, db_dsn: str, vars: Path):
    base_args = [
        "--collection-path",
        collection_path,
        "--database-dsn",
        db_dsn,
        "--vars",
        str(vars),
    ]
    runner = CliRunner()
    result = runner.invoke(init, base_args)
    assert result.exit_code == 0, result.output
    result = runner.invoke(edit, [*base_args, "service", "--host", "localhost"])
    assert result.exit_code == 0, result.output

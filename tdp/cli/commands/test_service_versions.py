# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.init import init
from tdp.cli.commands.service_versions import service_versions


def test_tdp_service_versions(
    collection_path: Path, database_dsn_path: str, vars: Path
):
    args = [
        "--collection-path",
        collection_path,
        "--database-dsn",
        database_dsn_path,
        "--vars",
        vars,
    ]
    runner = CliRunner()
    result = runner.invoke(init, args)
    assert result.exit_code == 0, result.output
    result = runner.invoke(service_versions, ["--database-dsn", database_dsn_path])
    assert result.exit_code == 0, result.output

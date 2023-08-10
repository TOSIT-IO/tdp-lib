# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.default_diff import default_diff


def test_tdp_default_diff(collection_path: Path, vars: Path):
    args = [
        "--collection-path",
        collection_path,
        "--vars",
        vars,
    ]
    runner = CliRunner()
    result = runner.invoke(default_diff, args)
    assert result.exit_code == 0, result.output

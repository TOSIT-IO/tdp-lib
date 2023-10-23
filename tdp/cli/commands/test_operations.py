# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.operations import ops


def test_tdp_nodes(collection_path: Path):
    args = ["--collection", collection_path]
    runner = CliRunner()
    result = runner.invoke(ops, args)
    assert result.exit_code == 0, result.output

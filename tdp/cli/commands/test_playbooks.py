# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.playbooks import playbooks


def test_tdp_playbooks(collection_path: Path, tmp_path: Path):
    args = ["--collection-path", collection_path, "--output-dir", tmp_path]
    runner = CliRunner()
    result = runner.invoke(playbooks, args)
    assert result.exit_code == 0

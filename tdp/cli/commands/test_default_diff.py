# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from click.testing import CliRunner
from .default_diff import default_diff


def test_tdp_default_diff(collection_path, vars):
    args = [
        "--collection-path",
        collection_path,
        "--vars",
        vars,
    ]
    runner = CliRunner()
    result = runner.invoke(default_diff, args)
    assert result.exit_code == 0

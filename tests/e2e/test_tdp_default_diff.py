# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.cli.commands.default_diff import default_diff


def test_tdp_default_diff(runner, collection_path, vars):
    result = runner.invoke(
        default_diff, f"--collection-path {collection_path} --vars {vars}".split()
    )
    assert result.exit_code == 0, result.output

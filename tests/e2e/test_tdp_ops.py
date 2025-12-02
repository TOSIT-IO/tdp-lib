# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.cli.commands.ops import ops


def test_tdp_nodes(runner, collection_path):
    result = runner.invoke(ops, f"--collection-path {collection_path}".split())
    assert result.exit_code == 0, result.output

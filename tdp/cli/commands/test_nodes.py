# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from click.testing import CliRunner
from .nodes import nodes


def test_tdp_nodes(collection_path):
    args = ["--collection-path", collection_path]
    runner = CliRunner()
    result = runner.invoke(nodes, args)
    assert result.exit_code == 0

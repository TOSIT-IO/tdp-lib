# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.cli.commands.vars.validate import validate


def test_tdp_validate(runner, collection_path, vars):
    result = runner.invoke(
        validate, f"--collection-path {collection_path} --vars {vars}".split()
    )
    assert result.exit_code == 0, result.output

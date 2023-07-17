# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from click.testing import CliRunner
from .validate import validate


def test_tdp_validate(collection_path, vars):
    args = ["--collection-path", collection_path, "--vars", vars]
    runner = CliRunner()
    result = runner.invoke(validate, args)
    assert result.exit_code == 0

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from click.testing import CliRunner

from ..init import init
from .reconfigure import reconfigure


def test_tdp_plan_reconfigure(collection_path, database_dsn_path, vars):
    base_args = [
        "--collection-path",
        collection_path,
        "--database-dsn",
        database_dsn_path,
    ]
    init_args = base_args + [
        "--vars",
        vars,
    ]
    runner = CliRunner()
    result = runner.invoke(init, init_args)
    assert result.exit_code == 0
    result = runner.invoke(reconfigure, base_args)
    assert result.exit_code == 1  # No stale components, hence nothing to reconfigure.

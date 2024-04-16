# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.init import init
from tdp.cli.commands.plan.ops import ops


def test_tdp_plan_run(collection_path: Path, db_dsn: str, vars: Path):
    base_args = [
        "--collection-path",
        collection_path,
        "--database-dsn",
        db_dsn,
    ]
    runner = CliRunner()
    result = runner.invoke(init, [*base_args, "--vars", str(vars)])
    assert result.exit_code == 0, result.output
    result = runner.invoke(ops, [*base_args, "service_install"])
    assert result.exit_code == 0, result.output

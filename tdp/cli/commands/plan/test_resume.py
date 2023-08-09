# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.init import init
from tdp.cli.commands.plan.dag import dag
from tdp.cli.commands.plan.resume import resume


def test_tdp_plan_resume_nothing_to_resume(
    collection_path: Path, database_dsn_path: str, vars: Path
):
    args = [
        "--collection-path",
        collection_path,
        "--database-dsn",
        database_dsn_path,
        "--vars",
        vars,
    ]
    runner = CliRunner()
    result = runner.invoke(init, args)
    assert result.exit_code == 0
    result = runner.invoke(dag, args)
    assert result.exit_code == 0
    result = runner.invoke(resume, args)
    assert result.exit_code == 1

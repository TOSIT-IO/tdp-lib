# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.init import init
from tdp.cli.commands.plan.dag import dag
from tdp.cli.commands.plan.resume import resume


def test_tdp_plan_resume_nothing_to_resume(
    collection_path: Path, database_dsn: str, vars: Path
):
    base_args = [
        "--collection-path",
        collection_path,
        "--database-dsn",
        database_dsn,
    ]
    runner = CliRunner()
    result = runner.invoke(init, [*base_args, "--vars", str(vars)])
    assert result.exit_code == 0, result.output
    result = runner.invoke(dag, base_args)
    assert result.exit_code == 0, result.output
    result = runner.invoke(resume, base_args)
    assert result.exit_code == 1, result.output  # No deployment to resume.

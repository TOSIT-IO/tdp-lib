# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.conftest import tdp_init_args
from tdp.cli.commands.plan.dag import dag
from tdp.cli.commands.plan.resume import resume


def test_tdp_plan_resume_nothing_to_resume(
    tdp_init: tdp_init_args,
):
    base_args = [
        "--collection-path",
        tdp_init.collection_path,
        "--database-dsn",
        tdp_init.db_dsn,
    ]
    runner = CliRunner()
    result = runner.invoke(dag, base_args)
    assert result.exit_code == 0, result.output
    result = runner.invoke(resume, base_args)
    assert result.exit_code == 1, result.output  # No deployment to resume.

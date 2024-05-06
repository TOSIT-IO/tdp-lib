# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.plan.dag import dag
from tdp.cli.commands.plan.resume import resume


def test_tdp_plan_resume_nothing_to_resume(
    tdp_init: tuple,
):
    tdp_init_args = [
        "--collection-path",
        tdp_init[0],
        "--database-dsn",
        tdp_init[1],
    ]
    runner = CliRunner()
    result = runner.invoke(dag, tdp_init_args)
    assert result.exit_code == 0, result.output
    result = runner.invoke(resume, tdp_init_args)
    assert result.exit_code == 1, result.output  # No deployment to resume.

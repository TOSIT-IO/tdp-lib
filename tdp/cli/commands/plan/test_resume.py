# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from click.testing import CliRunner

from tdp.cli.commands.conftest import TDPInitArgs
from tdp.cli.commands.plan.dag import dag
from tdp.cli.commands.plan.resume import resume


def test_tdp_plan_resume_nothing_to_resume(
    tdp_init: TDPInitArgs,
):
    common_args = [
        "--collection-path",
        str(tdp_init.collection_path),
        "--database-dsn",
        tdp_init.db_dsn,
    ]
    runner = CliRunner()
    result = runner.invoke(dag, common_args)
    assert result.exit_code == 0, result.output
    result = runner.invoke(resume, common_args)
    assert result.exit_code == 1, result.output  # No deployment to resume.

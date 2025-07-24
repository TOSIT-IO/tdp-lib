# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.cli.commands.plan.resume import resume


def test_tdp_plan_resume_nothing_to_resume(tdp, runner, collection_path, db_dsn, vars):
    result = tdp(
        f"init --collection-path {collection_path} --vars {vars} --database-dsn {db_dsn}"
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke(
        resume, f"--collection-path {collection_path} --database-dsn {db_dsn}".split()
    )
    assert result.exit_code == 1, result.output  # No deployment to resume.

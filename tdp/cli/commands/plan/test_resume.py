# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from click.testing import CliRunner
from .resume import resume
from .dag import dag
from ..init import init


def test_tdp_plan_resume_nothing_to_resume(collection_path, database_dsn_path, vars):
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

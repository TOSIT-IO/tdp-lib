# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.cli.commands.plan.dag import dag


def test_tdp_plan_dag(tdp, runner, collection_path, db_dsn, vars):
    collection_path.init_dag_directory(
        {
            "service": [
                {"name": "service_install"},
            ]
        }
    )
    result = tdp(
        f"init --collection-path {collection_path} --vars {vars} --database-dsn {db_dsn}"
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke(
        dag, f"--collection-path {collection_path} --database-dsn {db_dsn}".split()
    )
    assert result.exit_code == 0, result.output

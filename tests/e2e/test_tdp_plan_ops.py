# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.cli.commands.plan.ops import ops


def test_tdp_plan_run(tdp, vars, runner, collection_path, db_dsn):
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
        ops,
        f"--collection-path {collection_path} --database-dsn {db_dsn} service_install".split(),
    )
    assert result.exit_code == 0, result.output

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.cli.commands.deploy import deploy


def test_tdp_deploy_mock(runner, tdp, collection_path, db_dsn, vars):
    collection_path.init_dag_directory(
        {
            "service": [
                {"name": "service_install"},
            ],
        }
    )
    collection_path.init_default_vars_directory(
        {
            "service": {
                "service.yml": {},
            },
        }
    )

    result = tdp(
        f"init --collection-path {collection_path} --database-dsn {db_dsn} --vars {vars}"
    )
    assert result.exit_code == 0, result.output
    result = tdp(
        f"plan dag --collection-path {collection_path} --database-dsn {db_dsn}"
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke(
        deploy,
        f"--collection-path {collection_path} --database-dsn {db_dsn} --vars {vars}".split(),
    )
    assert result.exit_code == 0, result.output

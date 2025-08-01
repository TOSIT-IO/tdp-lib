# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.cli.commands.plan.reconfigure import reconfigure


def test_tdp_plan_reconfigure(runner, collection_path, db_dsn):
    result = runner.invoke(
        reconfigure,
        f"--collection-path {collection_path} --database-dsn {db_dsn}".split(),
    )
    assert result.exit_code == 1, result.output

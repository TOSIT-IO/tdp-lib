# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.cli.commands.status.show import show


def test_tdp_status_edit(tdp, runner, collection_path, db_dsn, vars):
    result = tdp(
        f"init --collection-path {collection_path} --vars {vars} --database-dsn {db_dsn}"
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke(
        show,
        f"--collection-path {collection_path} --database-dsn {db_dsn} --vars {vars}".split(),
    )
    assert result.exit_code == 0, result.output

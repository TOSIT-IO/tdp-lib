# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

from click.testing import CliRunner

from tdp.cli.commands.init import init


def test_tdp_init_db_is_created(collection_path: Path, vars: Path, tmp_path: Path):
    db_path = tmp_path / "sqlite.db"
    args = [
        "--collection-path",
        str(collection_path),
        "--database-dsn",
        "sqlite:///" + str(db_path),
        "--vars",
        str(vars),
    ]
    runner = CliRunner()
    result = runner.invoke(init, args)
    assert os.path.exists(db_path) == True
    assert result.exit_code == 0, result.output


def test_tdp_init_invalid_override_does_not_create_vars_directory(
    collection_path: Path, tmp_path: Path
):
    override_path = tmp_path / "overrides"
    override_service_path = override_path / "service"
    override_service_path.mkdir(parents=True)
    (override_service_path / "service.yml").write_text("invalid: [yaml")

    db_path = tmp_path / "sqlite.db"
    vars_path = tmp_path / "tdp_vars"
    args = [
        "--collection-path",
        str(collection_path),
        "--conf",
        str(override_path),
        "--database-dsn",
        "sqlite:///" + str(db_path),
        "--vars",
        str(vars_path),
    ]

    result = CliRunner().invoke(init, args)

    assert result.exit_code != 0
    assert not vars_path.exists()

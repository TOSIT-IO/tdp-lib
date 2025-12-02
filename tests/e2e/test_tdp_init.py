# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.cli.commands.init import init
from tdp.core.repository.git_repository import GitRepository


def test_tdp_init_missing_collection_path(runner, db_dsn, vars):
    """Test that the init command fails when collection path is not provided."""
    result = runner.invoke(init, f"--database-dsn {db_dsn} --vars {vars}".split())
    assert result.exit_code != 0
    assert "Error: Missing option '--collection-path'." in result.output


def test_tdp_init_missing_database_dsn(runner, collection_path, vars):
    """Test that the init command fails when database DSN is not provided."""
    result = runner.invoke(
        init, f"--collection-path {collection_path} --vars {vars}".split()
    )
    assert result.exit_code != 0
    assert "Error: Missing option '--database-dsn'." in result.output


def test_tdp_init_missing_vars_dir(runner, collection_path, db_dsn):
    """Test that the init command fails when vars directory is not provided."""
    result = runner.invoke(
        init, f"--collection-path {collection_path} --database-dsn {db_dsn}".split()
    )
    assert result.exit_code != 0
    assert "Error: Missing option '--vars'." in result.output


def test_tdp_init_variables_single_collection(runner, vars, db_dsn, collection_path):
    """Test that the init command runs successfully with valid parameters and service."""
    collection = collection_path
    collection.init_default_vars_directory(
        {
            "s1": {
                "s1.yml": {"foo": "value"},
            }
        }
    )

    result = runner.invoke(
        init,
        f"--collection-path {collection} --database-dsn {db_dsn} --vars {vars}".split(),
    )
    assert result.exit_code == 0, result.output
    assert vars.joinpath("s1", "s1.yml").exists()
    assert GitRepository(vars / "s1"), "Git repository should be initialized in vars/s1"
    repo = GitRepository(vars / "s1")
    assert repo.is_clean(), "Git repository should be clean after initialization"
    assert repo.current_version(), "Git repository should have a current version"
    assert len(list(repo._repo.iter_commits())) == 1, (
        "Git repository should have one commit after initialization"
    )
    assert vars.joinpath("s1", "s1.yml").read_text() == "foo: value\n"


def test_tdp_init_variables_multiple_collections(
    runner, collection_path_factory, vars, db_dsn
):
    """Test that the init command runs successfully with valid parameters and multiple collections."""
    collection1 = collection_path_factory()
    collection1.init_default_vars_directory(
        {
            "s1": {
                "s1.yml": {"foo": "value", "bar": "other value"},
            }
        }
    )
    collection2 = collection_path_factory()
    collection2.init_default_vars_directory(
        {
            "s1": {
                "s1.yml": {"foo": "new value"},
            },
            "s2": {
                "s2_c1.yml": {"baz": "value"},
            },
        }
    )

    result = runner.invoke(
        init,
        f"--collection-path {collection1} --collection-path {collection2} --database-dsn {db_dsn} --vars {vars}".split(),
    )
    assert result.exit_code == 0, result.output
    assert vars.joinpath("s1", "s1.yml").exists()
    assert vars.joinpath("s2", "s2_c1.yml").exists()
    assert GitRepository(vars / "s1"), "Git repository should be initialized in vars/s1"
    assert GitRepository(vars / "s2"), "Git repository should be initialized in vars/s2"
    repo_s1 = GitRepository(vars / "s1")
    repo_s2 = GitRepository(vars / "s2")
    assert repo_s1.is_clean(), "Git repository s1 should be clean after initialization"
    assert repo_s2.is_clean(), "Git repository s2 should be clean after initialization"
    assert repo_s1.current_version(), "Git repository s1 should have a current version"
    assert repo_s2.current_version(), "Git repository s2 should have a current version"
    assert len(list(repo_s1._repo.iter_commits())) == 2, (
        "Git repository s1 should have one commit after initialization"
    )
    assert len(list(repo_s2._repo.iter_commits())) == 1, (
        "Git repository s2 should have one commit after initialization"
    )
    assert (
        vars.joinpath("s1", "s1.yml").read_text()
        == "foo: new value\nbar: other value\n"
    )

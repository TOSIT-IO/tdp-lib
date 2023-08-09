# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest
from git import Repo

from tdp.core.repository.git_repository import (
    EmptyCommit,
    GitRepository,
    NotARepository,
)


@pytest.fixture
def git_repository(tmp_path):
    """Return a GitRepository instance."""
    git_repository = GitRepository.init(tmp_path)

    return git_repository


# https://stackoverflow.com/a/40884093
@pytest.fixture
def git_commit_empty_tree():
    """Return the hash of an empty tree commit."""
    return "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


def test_git_repository_fails_if_path_is_not_a_git_repo(tmp_path: Path):
    with pytest.raises(NotARepository):
        git_repository = GitRepository(tmp_path)
        with git_repository.validate("test commit"):
            pass


def test_git_repository_is_validated(git_repository: GitRepository):
    commit_message = "hive: update nb cores"
    group_vars = git_repository.path / "group_vars"
    hive_yml = "group_vars/hive.yml"
    group_vars.mkdir()
    with git_repository.validate(commit_message) as repository:
        with (repository.path / hive_yml).open("w") as hive_fd:
            hive_fd.write("nb_cores: 2")
        repository.add_for_validation([hive_yml])

    with Repo(git_repository.path) as repo:
        assert not repo.is_dirty()
        last_commit = repo.head.commit
        assert last_commit.message == commit_message
        assert hive_yml in last_commit.stats.files


def test_git_repository_multiple_validations(git_repository: GitRepository):
    group_vars = git_repository.path / "group_vars"
    group_vars.mkdir()
    file_list = [
        "group_vars/hive_s2.yml",
        "group_vars/hive_metastore.yml",
    ]
    with git_repository.validate("[HIVE] setup hive high availability") as repository:
        with (repository.path / file_list[0]).open("w") as hive_s2_fd, (
            repository.path / file_list[1]
        ).open("w") as hive_metastore_fd:
            hive_s2_fd.write(
                """
            hive_site:
              nb_hiveserver2: 0
            """
            )
            hive_metastore_fd.write("nb_threads: 4")
        repository.add_for_validation(file_list)

    with Repo(git_repository.path) as repo:
        assert not repo.is_dirty()
        last_commit = repo.head.commit
        assert len(file_list) == len(last_commit.stats.files)
        assert set(file_list) == set(last_commit.stats.files)


def test_git_repository_file_added(
    git_repository: GitRepository, git_commit_empty_tree: str
):
    file_name = "foo"
    with Path(git_repository.path, file_name).open("w") as fd:
        fd.write("foo\n")

    with Repo(git_repository.path) as repo:
        repo.index.add(file_name)
        repo.index.commit("add file")

    assert git_repository.is_file_modified(git_commit_empty_tree, file_name)


def test_git_repository_files_added_and_removed(
    git_repository: GitRepository, git_commit_empty_tree: str
):
    file_name = "foo"
    with Path(git_repository.path, file_name).open("w") as fd:
        fd.write("foo\n")

    with Repo(git_repository.path) as repo:
        repo.index.add(file_name)
        repo.index.commit("add files")
        repo.index.remove([file_name])
        repo.index.commit("remove first file")

    assert not git_repository.is_file_modified(git_commit_empty_tree, file_name)


def test_git_repository_file_updated(
    git_repository: GitRepository, git_commit_empty_tree: str
):
    file_name = "foo"
    with Path(git_repository.path, file_name).open("w") as fd:
        fd.write("foo\n")

    with Repo(git_repository.path) as repo:
        repo.index.add(file_name)

    with Path(git_repository.path, file_name).open("w") as fd:
        fd.write("foobar\n")

    with Repo(git_repository.path) as repo:
        repo.index.add([file_name])
        repo.index.commit("update first file")

    assert git_repository.is_file_modified(git_commit_empty_tree, file_name)


def test_git_repository_no_validation(git_repository: GitRepository):
    # first commit sets the value
    commit_message = "[HIVE] set nb cores"
    group_vars = git_repository.path / "group_vars"
    group_vars.mkdir()
    hive_yml = "group_vars/hive.yml"
    with git_repository.validate(commit_message) as repository:
        with (repository.path / hive_yml).open("w") as hive_fd:
            hive_fd.write("nb_core: 2")
        repository.add_for_validation([hive_yml])

    # second commit does not change anything
    commit_message_mock = "[HIVE] no changes"
    hive_yml = "group_vars/hive.yml"
    with pytest.raises(EmptyCommit):
        with git_repository.validate(commit_message) as repository:
            with open(repository.path / hive_yml, "w") as hive_fd:
                hive_fd.write("nb_core: 2")
            repository.add_for_validation([hive_yml])

    with Repo(git_repository.path) as repo:
        assert not repo.is_dirty()
        last_commit = repo.head.commit
        assert last_commit.message == commit_message

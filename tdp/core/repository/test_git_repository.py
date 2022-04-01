# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest
from git import Repo

from tdp.core.repository.git_repository import GitRepository, NotARepository


@pytest.fixture(scope="function")
def git_repository(tmp_path):
    git_repository = GitRepository.init(tmp_path)

    return git_repository


# https://stackoverflow.com/a/40884093
@pytest.fixture(scope="function")
def git_commit_empty_tree():
    return "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


def test_git_repository_fails_if_path_is_not_a_git_repo(tmp_path):
    with pytest.raises(NotARepository):
        git_repository = GitRepository(tmp_path)
        with git_repository.validate("test commit"):
            pass


def test_git_repository_is_validated(git_repository):
    commit_message = "[HIVE] update nb cores"
    hive_yml = "group_vars/hive.yml"
    with git_repository.validate(
        commit_message
    ) as repository, repository.open_var_file(hive_yml) as hive:
        hive["nb_cores"] = 2

    with Repo(git_repository.path) as repo:
        assert not repo.is_dirty()
        last_commit = repo.head.commit
        assert last_commit.message == commit_message
        assert hive_yml in last_commit.stats.files


def test_git_repository_multiple_validations(git_repository):
    file_list = [
        "group_vars/hive_s2.yml",
        "group_vars/hive_metastore.yml",
    ]
    with git_repository.validate(
        "[HIVE] setup hive high availability"
    ) as repository, repository.open_var_files(file_list) as confs:
        confs["group_vars/hive_s2.yml"].update({"hive_site": {"nb_hiveserver2": 8}})
        confs["group_vars/hive_metastore.yml"].update({"nb_threads": 4})

    with Repo(git_repository.path) as repo:
        assert not repo.is_dirty()
        last_commit = repo.head.commit
        assert len(file_list) == len(last_commit.stats.files)
        assert set(file_list) == set(last_commit.stats.files)


def test_git_repository_files_added(git_repository, git_commit_empty_tree):
    file_list = [
        "foo",
        "bar",
    ]
    with Path(git_repository.path, file_list[0]).open("w") as fd:
        fd.write("foo\n")
    with Path(git_repository.path, file_list[1]).open("w") as fd:
        fd.write("bar\n")

    with Repo(git_repository.path) as repo:
        repo.index.add(file_list)
        repo.index.commit("add files")

    assert sorted(git_repository.files_modified(git_commit_empty_tree)) == sorted(
        file_list
    )


def test_git_repository_files_added_and_removed(git_repository, git_commit_empty_tree):
    file_list = [
        "foo",
        "bar",
    ]
    with Path(git_repository.path, file_list[0]).open("w") as fd:
        fd.write("foo\n")
    with Path(git_repository.path, file_list[1]).open("w") as fd:
        fd.write("bar\n")

    with Repo(git_repository.path) as repo:
        repo.index.add(file_list)
        repo.index.commit("add files")
        repo.index.remove([file_list[0]])
        repo.index.commit("remove first file")

    assert sorted(git_repository.files_modified(git_commit_empty_tree)) == sorted(
        [file_list[1]]
    )


def test_git_repository_file_updated(git_repository):
    file_list = [
        "foo",
        "bar",
    ]
    with Path(git_repository.path, file_list[0]).open("w") as fd:
        fd.write("foo\n")
    with Path(git_repository.path, file_list[1]).open("w") as fd:
        fd.write("bar\n")

    initial_commit = None
    with Repo(git_repository.path) as repo:
        repo.index.add(file_list)
        initial_commit = str(repo.index.commit("add files"))

    with Path(git_repository.path, file_list[0]).open("w") as fd:
        fd.write("foobar\n")

    with Repo(git_repository.path) as repo:
        repo.index.add([file_list[0]])
        repo.index.commit("update first file")

    assert sorted(git_repository.files_modified(initial_commit)) == sorted(
        [file_list[0]]
    )

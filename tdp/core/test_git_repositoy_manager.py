import pytest

from git import Repo, InvalidGitRepositoryError

from tdp.core.git_repository import GitRepository

from tdp.core.repository_manager import RepositoryManager


@pytest.fixture(scope="function")
def repository_manager(tmp_path):
    repository_manager = RepositoryManager(tmp_path, repository_class=GitRepository)
    repository_manager.init()

    return repository_manager


def test_repository_manager_fails_if_path_is_not_a_git_repo(tmp_path):
    with pytest.raises(InvalidGitRepositoryError):
        repository_manager = RepositoryManager(tmp_path, repository_class=GitRepository)
        with repository_manager.validate("test commit"):
            pass


def test_repository_manager_is_validated(repository_manager):
    commit_message = "[HIVE] update nb cores"
    hive_yml = "group_vars/hive.yml"
    with repository_manager.validate(
        commit_message
    ) as repository, repository.open_var_file(hive_yml) as hive:
        hive["nb_cores"] = 2

    with Repo(repository_manager.path) as repo:
        assert not repo.is_dirty()
        last_commit = repo.head.commit
        assert last_commit.message == commit_message
        assert hive_yml in last_commit.stats.files


def test_repository_manager_multiple_validations(repository_manager):
    file_list = [
        "group_vars/hive_s2.yml",
        "group_vars/hive_metastore.yml",
    ]
    with repository_manager.validate(
        "[HIVE] setup hive high availability"
    ) as repository, repository.open_var_files(file_list) as confs:
        confs["group_vars/hive_s2.yml"].update({"hive_site": {"nb_hiveserver2": 8}})
        confs["group_vars/hive_metastore.yml"].update({"nb_threads": 4})

    with Repo(repository_manager.path) as repo:
        assert not repo.is_dirty()
        last_commit = repo.head.commit
        assert len(file_list) == len(last_commit.stats.files)
        assert set(file_list) == set(last_commit.stats.files)

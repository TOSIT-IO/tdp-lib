import pytest

from git import Repo, InvalidGitRepositoryError

from tdp.core.repository.git_repository import GitRepository


@pytest.fixture(scope="function")
def git_repository(tmp_path):
    GitRepository.init(tmp_path)
    git_repository = GitRepository(tmp_path)

    return git_repository


def test_git_repository_fails_if_path_is_not_a_git_repo(tmp_path):
    with pytest.raises(InvalidGitRepositoryError):
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

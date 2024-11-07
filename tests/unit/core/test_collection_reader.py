# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest
from pydantic import ValidationError

from tdp.core.collection_reader import (
    CollectionReader,
    MissingMandatoryDirectoryError,
    PathDoesNotExistsError,
    PathIsNotADirectoryError,
    read_hosts_from_playbook,
)
from tdp.core.constants import (
    DAG_DIRECTORY_NAME,
    DEFAULT_VARS_DIRECTORY_NAME,
    PLAYBOOKS_DIRECTORY_NAME,
)
from tests.conftest import generate_collection_at_path
from tests.unit.core.models.test_deployment_log import (
    MockInventoryReader,
)


@pytest.fixture(scope="session")
def mock_empty_collection_reader(tmp_path_factory: pytest.TempPathFactory) -> Path:
    temp_collection_path = tmp_path_factory.mktemp("mock_collection")
    for directory in [
        DAG_DIRECTORY_NAME,
        DEFAULT_VARS_DIRECTORY_NAME,
        PLAYBOOKS_DIRECTORY_NAME,
    ]:
        (temp_collection_path / directory).mkdir(parents=True, exist_ok=True)
    return temp_collection_path


def test_collection_from_path_does_not_exist():
    with pytest.raises(PathDoesNotExistsError):
        CollectionReader.from_path("foo")


def test_collection_from_path_is_not_a_directory(tmp_path: Path):
    empty_file = tmp_path / "foo"
    empty_file.touch()
    with pytest.raises(PathIsNotADirectoryError):
        CollectionReader.from_path(empty_file)


def test_collection_from_path_missing_mandatory_directory(tmp_path: Path):
    with pytest.raises(MissingMandatoryDirectoryError):
        CollectionReader.from_path(tmp_path)


def test_collection_from_path(tmp_path_factory: pytest.TempPathFactory):
    collection_path = tmp_path_factory.mktemp("collection")
    dag_service_operations = {
        "service": [
            {"name": "service_install"},
            {"name": "service_config"},
        ],
    }
    service_vars = {
        "service": {
            "service": {},
        },
    }
    generate_collection_at_path(collection_path, dag_service_operations, service_vars)
    playbooks = CollectionReader.from_path(collection_path).read_playbooks()
    assert "service_install" in playbooks
    assert "service_config" in playbooks


def test_read_hosts_from_playbook(tmp_path: Path):
    playbook_path = tmp_path / "playbook.yml"
    playbook_path.write_text(
        """---
- name: Play 1
  hosts: host1, host2
  tasks:
    - name: Task 1
      command: echo "Hello, World!"

"""
    )
    hosts = read_hosts_from_playbook(
        playbook_path, MockInventoryReader(["host1", "host2"])
    )
    assert hosts == {"host1", "host2"}


def test_collection_reader_read_playbooks(mock_empty_collection_reader: Path):
    collection_reader = CollectionReader(mock_empty_collection_reader)
    playbook_path_1 = collection_reader.playbooks_directory / "playbook1.yml"
    playbook_path_2 = collection_reader.playbooks_directory / "playbook2.yml"
    playbook_path_1.write_text(
        """---
- name: Play 1
  hosts: host1, host2
  tasks:
    - name: Task 1
      command: echo "Hello, World!"
"""
    )
    playbook_path_2.write_text(
        """---
- name: Play 2
  hosts: host3, host4
  tasks:
    - name: Task 2
      command: echo "Hello, GitHub Copilot!"
"""
    )
    playbooks = collection_reader.read_playbooks()
    assert len(playbooks) == 2
    assert "playbook1" in playbooks
    assert "playbook2" in playbooks
    assert playbooks["playbook1"].path == playbook_path_1
    assert playbooks["playbook1"].collection_name == collection_reader.name
    assert playbooks["playbook2"].path == playbook_path_2
    assert playbooks["playbook2"].collection_name == collection_reader.name


def test_collection_reader_read_dag_nodes(mock_empty_collection_reader: Path):
    collection_reader = CollectionReader(mock_empty_collection_reader)
    dag_file_1 = collection_reader.dag_directory / "dag1.yml"
    dag_file_2 = collection_reader.dag_directory / "dag2.yml"
    dag_file_1.write_text(
        """---
- name: s1_c1_a
  depends_on:
    - sx_cx_a
"""
    )
    dag_file_2.write_text(
        """---
- name: s2_c2_a
  depends_on:
    - s1_c1_a
"""
    )
    dag_nodes = list(collection_reader.read_dag_nodes())
    assert len(dag_nodes) == 2
    assert any(
        node.name == "s1_c1_a" and node.depends_on == ["sx_cx_a"] for node in dag_nodes
    )
    assert any(
        node.name == "s2_c2_a" and node.depends_on == ["s1_c1_a"] for node in dag_nodes
    )


def test_collection_reader_read_dag_nodes_empty_file(
    mock_empty_collection_reader: Path,
):
    collection_reader = CollectionReader(mock_empty_collection_reader)
    dag_file = collection_reader.dag_directory / "dag.yml"
    dag_file.write_text("")
    with pytest.raises(ValidationError):
        list(collection_reader.read_dag_nodes())

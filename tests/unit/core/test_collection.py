# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest
from pydantic import ValidationError

from tdp.core.collection import (
    CollectionReader,
    MissingMandatoryDirectoryError,
    PathDoesNotExistsError,
    PathIsNotADirectoryError,
    check_collection_structure,
    read_dag_directory,
    read_dag_file,
    read_hosts_from_playbook,
    read_playbooks_directory,
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


def test_init_collection_playbooks(tmp_path: Path):
    collection_path = tmp_path / "collection"
    playbook_directory = "playbooks"
    (playbook_directory_path := collection_path / playbook_directory).mkdir(
        parents=True, exist_ok=True
    )
    playbook_path_1 = playbook_directory_path / "playbook1.yml"
    playbook_path_2 = playbook_directory_path / "playbook2.yml"
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
    playbooks = read_playbooks_directory(playbook_directory_path, collection_path.name)
    assert len(playbooks) == 2
    assert "playbook1" in playbooks
    assert "playbook2" in playbooks
    assert playbooks["playbook1"].path == playbook_path_1
    assert playbooks["playbook1"].collection_name == collection_path.name
    assert playbooks["playbook2"].path == playbook_path_2
    assert playbooks["playbook2"].collection_name == collection_path.name


def test_check_collection_structure_path_does_not_exist(tmp_path: Path):
    with pytest.raises(PathDoesNotExistsError):
        check_collection_structure(tmp_path / "nonexistent_directory")


def test_check_collection_structure_path_is_not_a_directory(tmp_path: Path):
    empty_file = tmp_path / "foo"
    empty_file.touch()
    with pytest.raises(PathIsNotADirectoryError):
        check_collection_structure(empty_file)


def test_check_collection_structure_missing_mandatory_directory(tmp_path: Path):
    with pytest.raises(MissingMandatoryDirectoryError):
        check_collection_structure(tmp_path)


def test_check_collection_structure_valid_collection(tmp_path: Path):
    collection_path = tmp_path / "collection"
    for mandatory_directory in (
        DAG_DIRECTORY_NAME,
        DEFAULT_VARS_DIRECTORY_NAME,
        PLAYBOOKS_DIRECTORY_NAME,
    ):
        (collection_path / mandatory_directory).mkdir(parents=True, exist_ok=True)
    assert check_collection_structure(collection_path) is None


def test_read_dag_file(tmp_path: Path):
    dag_file_path = tmp_path / "dag_file.yml"
    dag_file_path.write_text(
        """---
- name: s1_c1_a
  depends_on:
    - sx_cx_a
- name: s2_c2_a
  depends_on:
    - s1_c1_a
- name: s3_c3_a
  depends_on:
    - sx_cx_a
    - sy_cy_a
"""
    )
    operations = list(read_dag_file(dag_file_path))
    assert len(operations) == 3
    assert operations[0].name == "s1_c1_a"
    assert operations[0].depends_on == ["sx_cx_a"]
    assert operations[1].name == "s2_c2_a"
    assert operations[1].depends_on == ["s1_c1_a"]
    assert operations[2].name == "s3_c3_a"
    assert operations[2].depends_on == ["sx_cx_a", "sy_cy_a"]


def test_read_dag_file_empty(tmp_path: Path):
    dag_file_path = tmp_path / "dag_file.yml"
    dag_file_path.write_text("")
    with pytest.raises(ValidationError):
        list(read_dag_file(dag_file_path))


def test_read_dag_file_with_additional_props(tmp_path: Path):
    dag_file_path = tmp_path / "dag_file.yml"
    dag_file_path.write_text(
        """---
- name: s1_c1_a
  depends_on:
    - sx_cx_a
  foo: bar
"""
    )
    operations = list(read_dag_file(dag_file_path))
    assert len(operations) == 1
    assert operations[0].name == "s1_c1_a"
    assert operations[0].depends_on == ["sx_cx_a"]


def test_get_collection_dag_nodes(tmp_path: Path):
    collection_path = tmp_path / "collection"
    dag_directory = "dag"
    (dag_directory_path := collection_path / dag_directory).mkdir(
        parents=True, exist_ok=True
    )
    dag_file_1 = dag_directory_path / "dag1.yml"
    dag_file_2 = dag_directory_path / "dag2.yml"
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
    dag_nodes = list(read_dag_directory(dag_directory_path))
    assert len(dag_nodes) == 2
    assert any(
        node.name == "s1_c1_a" and node.depends_on == ["sx_cx_a"] for node in dag_nodes
    )
    assert any(
        node.name == "s2_c2_a" and node.depends_on == ["s1_c1_a"] for node in dag_nodes
    )

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest
from pydantic import ValidationError

from tdp.core.collection.read_dag import read_dag_directory, read_dag_file


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

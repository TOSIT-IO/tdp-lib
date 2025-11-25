# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest.mock import Mock

import pytest

from tdp.core.constants import HOST_NAME_MAX_LENGTH
from tdp.core.entities.operation import Playbook


def test_playbook_creation(tmp_path: Path):
    path = tmp_path / "playbook.yml"
    collection_name = "my_collection"
    hosts = frozenset(["host1", "host2", "host3"])

    playbook = Playbook(
        path=path, collection_name=collection_name, hosts=hosts, meta=Mock()
    )

    assert playbook.path == path
    assert playbook.collection_name == collection_name
    assert playbook.hosts == hosts


def test_playbook_creation_with_long_host_name(tmp_path: Path):
    path = tmp_path / "playbook.yml"
    collection_name = "my_collection"
    long_host_name = "a" * (HOST_NAME_MAX_LENGTH + 1)
    hosts = frozenset(["host1", "host2", long_host_name])

    with pytest.raises(ValueError):
        Playbook(path=path, collection_name=collection_name, hosts=hosts, meta=Mock())

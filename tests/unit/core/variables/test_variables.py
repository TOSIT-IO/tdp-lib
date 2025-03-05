# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from ansible.inventory.manager import InventoryManager
from ansible.parsing.dataloader import DataLoader
from ansible.vars.manager import VariableManager

from tdp.core.variables.variables import Variables

_DummyInventory = tuple[DataLoader, InventoryManager, VariableManager, Path]


@pytest.fixture
def dummy_inventory(tmp_path: Path) -> Generator[_DummyInventory, Any, None]:
    group_vars = tmp_path / "group_vars"
    hdfs_vars = group_vars / "hdfs.yml"
    group_vars.mkdir()
    hdfs_vars.touch()

    loader = DataLoader()
    # inventory = InventoryManager(loader=loader, sources=C.DEFAULT_HOST_LIST)
    inventory = InventoryManager(loader=loader, sources=[str(tmp_path)])
    variable_manager = VariableManager(loader=loader, inventory=inventory)

    inventory.add_group("hdfs")
    inventory.add_group("hdfs_nn")
    inventory.add_group("hdfs_dn")

    inventory.add_host("master01")
    inventory.get_host("master01").add_group(inventory.groups["hdfs"])
    inventory.get_host("master01").add_group(inventory.groups["hdfs_nn"])

    inventory.reconcile_inventory()

    yield (loader, inventory, variable_manager, hdfs_vars)


def test_variables_update(dummy_inventory: _DummyInventory):
    (loader, inventory, variable_manager, hdfs_vars) = dummy_inventory
    with Variables(hdfs_vars).open() as variables:
        variables.update({"hdfs_property": "hdfs_value"})

    assert "hdfs_value" == variable_manager.get_vars(
        host=inventory.get_host("master01")
    ).get("hdfs_property")


def test_variables_unset(dummy_inventory: _DummyInventory):
    (loader, inventory, variable_manager, hdfs_vars) = dummy_inventory

    with Variables(hdfs_vars).open() as variables:
        variables.update(
            {
                "hdfs_property": "hdfs_value",
                "hdfs_site": {
                    "hdfs.nested.property": "hdfs_nested_value",
                    "hdfs.another.nested.property": "another_value",
                },
            },
        )
        del variables["hdfs_property"]

    assert "hdfs_property" not in variable_manager.get_vars(
        host=inventory.get_host("master01")
    )


def test_variables_unset_nested(dummy_inventory: _DummyInventory):
    (loader, inventory, variable_manager, hdfs_vars) = dummy_inventory

    with Variables(hdfs_vars).open() as variables:
        variables.update(
            {
                "hdfs_property": "hdfs_value",
                "hdfs_site": {
                    "hdfs.nested.property": "hdfs_nested_value",
                    "hdfs.another.nested.property": "another_value",
                },
            },
        )

        del variables["hdfs_site"]["hdfs.nested.property"]

    assert "hdfs.nested.property" not in variable_manager.get_vars(
        host=inventory.get_host("master01")
    ).get("hdfs_site")

    assert "another_value" == variable_manager.get_vars(
        host=inventory.get_host("master01")
    ).get("hdfs_site").get("hdfs.another.nested.property")


def test_variables_item_is_settable(dummy_inventory: _DummyInventory):
    (loader, inventory, variable_manager, hdfs_vars) = dummy_inventory
    with Variables(hdfs_vars).open() as variables:
        variables["hdfs_property"] = "hdfs_value"

    assert "hdfs_value" == variable_manager.get_vars(
        host=inventory.get_host("master01")
    ).get("hdfs_property")


def test_variables_item_is_gettable(dummy_inventory: _DummyInventory):
    (loader, inventory, variable_manager, hdfs_vars) = dummy_inventory
    with Variables(hdfs_vars).open() as variables:
        variables["hdfs_property"] = "hdfs_value"
        assert "hdfs_value" == variables["hdfs_property"]


def test_variables_item_is_deletable(dummy_inventory: _DummyInventory):
    (loader, inventory, variable_manager, hdfs_vars) = dummy_inventory

    with Variables(hdfs_vars).open() as variables:
        variables.update(
            {
                "hdfs_property": "hdfs_value",
                "hdfs_site": {
                    "hdfs.nested.property": "hdfs_nested_value",
                    "hdfs.another.nested.property": "another_value",
                },
            },
        )
        del variables["hdfs_property"]

    assert "hdfs_property" not in variable_manager.get_vars(
        host=inventory.get_host("master01")
    )


def test_skip_if_file_not_writable(dummy_inventory: _DummyInventory):
    (loader, inventory, variable_manager, hdfs_vars) = dummy_inventory

    with Variables(hdfs_vars).open("r"):
        pass

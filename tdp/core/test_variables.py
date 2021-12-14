from json import load
import os

import ansible.constants as C
from ansible.inventory.manager import InventoryManager
from ansible.vars.manager import VariableManager
from ansible.parsing.dataloader import DataLoader

import pytest

from tdp.core.variables import Variables


@pytest.fixture(scope="function")
def dummy_inventory(tmp_path):
    os.mkdir(os.path.join(tmp_path, "group_vars"))

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

    yield (loader, inventory, variable_manager, tmp_path)


def test_variables_update(dummy_inventory):
    (loader, inventory, variable_manager, tmp_path) = dummy_inventory
    variables = Variables(os.path.join(tmp_path, "group_vars"))

    variables.update("hdfs", {"hdfs_property": "hdfs_value"})

    assert "hdfs_value" == variable_manager.get_vars(
        host=inventory.get_host("master01")
    ).get("hdfs_property")


def test_variables_unset(dummy_inventory):
    (loader, inventory, variable_manager, tmp_path) = dummy_inventory

    variables = Variables(os.path.join(tmp_path, "group_vars"))

    variables.update(
        "hdfs",
        {
            "hdfs_property": "hdfs_value",
            "hdfs_site": {
                "hdfs.nested.property": "hdfs_nested_value",
                "hdfs.another.nested.property": "another_value",
            },
        },
    )

    variables.unset("hdfs", "hdfs_property")

    assert "hdfs_property" not in variable_manager.get_vars(
        host=inventory.get_host("master01")
    )


def test_variables_unset_nested(dummy_inventory):
    (loader, inventory, variable_manager, tmp_path) = dummy_inventory

    variables = Variables(os.path.join(tmp_path, "group_vars"))

    variables.update(
        "hdfs",
        {
            "hdfs_property": "hdfs_value",
            "hdfs_site": {
                "hdfs.nested.property": "hdfs_nested_value",
                "hdfs.another.nested.property": "another_value",
            },
        },
    )

    variables.unset("hdfs", "hdfs_site.hdfs.nested.property")

    assert "hdfs.nested.property" not in variable_manager.get_vars(
        host=inventory.get_host("master01")
    ).get("hdfs_site")

    assert "another_value" == variable_manager.get_vars(
        host=inventory.get_host("master01")
    ).get("hdfs_site").get("hdfs.another.nested.property")

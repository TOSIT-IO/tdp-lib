# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import argparse
import logging
import pathlib
from collections.abc import Generator, Iterable, Mapping
from pathlib import Path
from typing import Any, TextIO, Union

import yaml

from tdp.core.collections import Collections
from tdp.core.collections.collection_reader import (
    CollectionReader,
    MissingMandatoryDirectoryError,
    PathDoesNotExistsError,
    PathIsNotADirectoryError,
)
from tdp.core.constants import YML_EXTENSION
from tdp.core.entities.entity_name import (
    ServiceComponentName,
    ServiceName,
    parse_entity_name,
)
from tdp.core.inventory_reader import InventoryReader

from .constants import RULES_DIRECTORY_NAME

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)


# * Duplicated from tdp.core.models.test_deployment_log.py
class _MockInventoryReader(InventoryReader):
    """A mock InventoryReader which always return the same hosts"""

    def __init__(self, hosts: list[str]):
        self.hosts = hosts

    def get_hosts(self, *args, **kwargs) -> list[str]:
        return self.hosts

    def get_hosts_from_playbook(self, fd: TextIO) -> set[str]:
        return set(self.hosts)


class _Rule(Mapping[str, Any]):
    """A rule used to test the operations order of a service_component or service.

    A rules is defined by:
    - source: a source service_component, which will trigger the reconfiguration
    - must_include: a list service_component names to include in the reconfiguration
    - must_exclude: a list of service_component names to exclude from the
      reconfiguration
    """

    def __init__(
        self,
        source: str,
        must_include: frozenset[str],
        must_exclude: frozenset[str],
        rules_file_path: pathlib.Path,
    ):
        self._rules_file_path = rules_file_path
        self._source = source
        self._must_include = must_include
        self._must_exclude = must_exclude

    @staticmethod
    def from_rule_dict(
        source: str, rules: dict[str, Any], rules_file_path: pathlib.Path
    ):
        """Create a Rule from a dict read from a rules file"""
        return _Rule(
            source=source,
            must_include=frozenset(rules.get("must_include", [])),
            must_exclude=frozenset(rules.get("must_exclude", [])),
            rules_file_path=rules_file_path,
        )

    @property
    def collection_name(self) -> str:
        """Name of the collection this rule belongs to"""
        return self._rules_file_path.parent.parent.name

    @property
    def rule_file_name(self) -> str:
        """Name of the file this rule belongs to"""
        return self._rules_file_path.name

    @property
    def fixtures(self) -> dict[str, Any]:
        """Dict of fixtures names and values for this rule"""
        return {
            "source": self._source,
            "must_include": self._must_include,
            "must_exclude": self._must_exclude,
        }

    @property
    def id(self) -> str:
        """ID of the rule"""
        return f"{self.collection_name}:{self.rule_file_name}:{self._source}"

    def __getitem__(self, key: str) -> Any:
        return self.fixtures[key]

    def __iter__(self):
        return iter(self.fixtures)

    def __len__(self):
        return len(self.fixtures)


class CollectionToTest(CollectionReader):
    """A Collection containing a rules directory"""

    def __init__(self, path: Union[str, pathlib.Path]):
        super().__init__(path)

    @property
    def rules_directory(self) -> Path:
        """Path to the rules directory"""
        return self.path / RULES_DIRECTORY_NAME

    def get_rules(self) -> Generator[_Rule, None, None]:
        """Yields every rules from the rules directory"""
        if not self.rules_directory.exists():
            logger.warning(
                f"Rules directory {self.rules_directory} does not exists for",
                "collection {self.name}, skipping",
            )
            return
        logger.info(f"Loading rules for collection {self.name}")
        for rules_file_path in self.rules_directory.glob("*" + YML_EXTENSION):
            logger.debug(f"Loading rules from {rules_file_path}")
            with open(rules_file_path) as f:
                rules: dict = yaml.safe_load(f)
                for [source, rule_dict] in rules.items():
                    yield _Rule.from_rule_dict(
                        source=source, rules=rule_dict, rules_file_path=rules_file_path
                    )


class append_collection_list_action(argparse.Action):
    """argparse action to create a list of CollectionToTest from paths"""

    def __call__(self, parser, namespace, values: pathlib.Path, option_string=None):
        # Initialize the attribute if it doesn't exist
        if not hasattr(namespace, self.dest) or getattr(namespace, self.dest) is None:
            setattr(namespace, self.dest, [])
        # Append the path to the namespace
        try:
            collection = CollectionToTest(values)
            collection._inventory_reader = _MockInventoryReader(["localhost"])
            getattr(namespace, self.dest).append(collection)
        except (
            PathDoesNotExistsError,
            PathIsNotADirectoryError,
            MissingMandatoryDirectoryError,
        ) as e:
            parser.error(f"{e}")


def resolve_components(
    service_components: Iterable[str], collections: Collections
) -> tuple[frozenset[str], dict[str, str]]:
    """Resolve service and component names into their constituent components.

    This function takes a mixed list of service names and service component names
    and expands any service names into their individual components. It's used in
    testing scenarios where operations need to be resolved at the component level.

    When a service name is provided (e.g., "hdfs"), it gets expanded to all its
    components (e.g., "hdfs_namenode", "hdfs_datanode", etc.). When a component
    name is provided (e.g., "hdfs_namenode"), it's kept as-is.

    Args:
        service_components: An iterable of strings that can contain service names
            (e.g., "hdfs") or service component names (e.g., "hdfs_namenode").
        collections: Collections object containing the operations and their
            service/component relationships.

    Returns:
        A tuple containing:
        - A frozenset of resolved component names (all individual components)
        - A dictionary mapping component names to their parent service names
          (only for components that were expanded from services)

    Examples:
        >>> resolve_components(["service1", "service2"])
        (frozenset({"service1", "service2"}), {})
        >>> resolve_components(["service1", "service2_component1"])
        (frozenset({"service1", "service2_component1"}), {"service2_component1": "service2"})
        >>> resolve_components(["hdfs", "yarn_nodemanager"])
        (frozenset({"hdfs_namenode", "hdfs_datanode", "yarn_nodemanager"}),
         {"hdfs_namenode": "hdfs", "hdfs_datanode": "hdfs"})
    """
    resolved_components: set[str] = set()
    service_component_map: dict[str, str] = {}
    for service_component in service_components:
        if isinstance(parse_entity_name(service_component), ServiceName):
            # Find all components for this service by iterating through operations
            for operation in collections.operations.values():
                if operation.name.service == service_component and isinstance(
                    operation.name.entity, ServiceComponentName
                ):
                    component_name = operation.name.entity.name
                    resolved_components.add(component_name)
                    service_component_map[component_name] = service_component
        else:
            resolved_components.add(service_component)
    return frozenset(resolved_components), service_component_map

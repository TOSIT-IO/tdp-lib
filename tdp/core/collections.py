# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from collections import OrderedDict
from collections.abc import Mapping

import yaml

from tdp.core.operation import Operation

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

logger = logging.getLogger("tdp").getChild("collections")


class Collections(Mapping):
    def __init__(self, collections):
        self._collections = collections
        self._dag_operations = None
        self._other_operations = None
        self._init_operations()

    def __getitem__(self, key):
        return self._collections.__getitem__(key)

    def __iter__(self):
        return self._collections.__iter__()

    def __len__(self):
        return self._collections.__len__()

    @staticmethod
    def from_collection_list(collections):
        """Factory method to build Collections from Ordered Sequence of Collection object

        Ordering of the sequence is what will determine the loading order of the operations.
        An operation can override a previous operation.

        :param collections: Ordered Sequence of Collection
        :type collections: Sequence[Collection]
        :return: Collections built from x collections
        :rtype: Collections
        """
        collections = OrderedDict(
            (collection.name, collection) for collection in collections
        )

        return Collections(collections)

    @property
    def collections(self):
        return self._collections

    @collections.setter
    def collections(self, collections):
        self._collections = collections
        self._init_operations()

    @property
    def dag_operations(self):
        return self._dag_operations

    @property
    def other_operations(self):
        return self._other_operations

    @property
    def operations(self):
        operations = {}
        if self._dag_operations:
            operations.update(self._dag_operations)
        if self._other_operations:
            operations.update(self._other_operations)
        return operations

    def _init_operations(self):
        self._dag_operations = {}
        self._other_operations = {}

        # Init DAG Operations
        for collection_name, collection in self._collections.items():
            for yaml_file in collection.dag_yamls:
                operations_list = None
                with yaml_file.open("r") as operation_file:
                    operations_list = yaml.load(operation_file, Loader=Loader) or []

                for operation in operations_list:
                    name = operation["name"]
                    if name in self._dag_operations:
                        logger.info(
                            f"DAG Operation '{name}' defined in collection "
                            f"'{self._dag_operations[name].collection_name}' "
                            f"is overridden by collection '{collection_name}'"
                        )

                    self._dag_operations[name] = Operation(
                        collection_name=collection_name, **operation
                    )

        # Init Operations not in the DAG
        for collection_name, collection in self._collections.items():
            for operation_name, operation_file in collection.operations.items():
                if operation_name in self._dag_operations:
                    continue
                if operation_name in self._other_operations:
                    logger.info(
                        f"Operation '{operation_name}' defined in collection "
                        f"'{self._other_operations[operation_name].collection_name}' "
                        f"is overridden by collection '{collection_name}'"
                    )

                self._other_operations[operation_name] = Operation(
                    name=operation_name,
                    collection_name=collection_name,
                )

    def get_service_schemas(self, name):
        """return list of schema"""
        return list(
            filter(
                bool,
                (
                    collection.get_service_schema(name)
                    for collection in self._collections.values()
                ),
            )
        )

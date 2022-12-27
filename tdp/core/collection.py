# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import json
from pathlib import Path

DAG_DIRECTORY_NAME = "tdp_lib_dag"
OPERATION_DIRECTORY_NAME = "playbooks"
DEFAULT_VARS_DIRECTORY_NAME = "tdp_vars_defaults"
SCHEMA_VARS_DIRECTORY_NAME = "tdp_vars_schema"

JSON_EXTENSION = ".json"
YML_EXTENSION = ".yml"

MANDATORY_DIRECTORIES = [
    DAG_DIRECTORY_NAME,
    DEFAULT_VARS_DIRECTORY_NAME,
    OPERATION_DIRECTORY_NAME,
]


class Collection:
    def __init__(self, path):
        self._path = Path(path)
        self._dag_yamls = None
        self._operations = None

    @staticmethod
    def from_path(path):
        path = Path(path).expanduser().resolve()
        if not path.exists():
            raise ValueError(f"{path} does not exists")
        if not path.is_dir():
            raise ValueError(f"{path} is not a directory")
        for mandatory_directory in MANDATORY_DIRECTORIES:
            mandatory_path = path / mandatory_directory
            if not mandatory_path.exists() or not mandatory_path.is_dir():
                raise ValueError(
                    f"{path} does not contain the mandatory directory {mandatory_directory}",
                )
        return Collection(path)

    @property
    def path(self):
        return self._path

    @property
    def name(self):
        return self._path.name

    @property
    def dag_directory(self):
        return self._path / DAG_DIRECTORY_NAME

    @property
    def default_vars_directory(self):
        return self._path / DEFAULT_VARS_DIRECTORY_NAME

    @property
    def operations_directory(self):
        return self._path / OPERATION_DIRECTORY_NAME

    @property
    def schema_directory(self):
        return self._path / SCHEMA_VARS_DIRECTORY_NAME

    @property
    def dag_yamls(self):
        if not self._dag_yamls:
            self._dag_yamls = list(self.dag_directory.glob("*" + YML_EXTENSION))
        return self._dag_yamls

    @property
    def operations(self):
        if not self._operations:
            self._operations = {
                playbook.stem: playbook
                for playbook in self.operations_directory.glob("*" + YML_EXTENSION)
            }
        return self._operations

    def get_service_default_vars(self, name):
        service_path = self.default_vars_directory / name
        if not service_path.exists():
            return []
        return [(path.name, path) for path in service_path.glob("*" + YML_EXTENSION)]

    def get_service_schema(self, name):
        schema_path = self.schema_directory / (name + JSON_EXTENSION)
        if not schema_path.exists():
            return {}
        with schema_path.open() as fd:
            return json.load(fd)

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import List, Mapping

import yaml

from tdp.core.collection import (
    DAG_DIRECTORY_NAME,
    DEFAULT_VARS_DIRECTORY_NAME,
    PLAYBOOKS_DIRECTORY_NAME,
    SCHEMA_VARS_DIRECTORY_NAME,
)


def generate_collection(
    directory: Path,
    dag_service_operations: Mapping[str, List],
    service_vars: Mapping[str, Mapping[str, dict]],
):
    tdp_lib_dag = directory / DAG_DIRECTORY_NAME
    playbooks = directory / PLAYBOOKS_DIRECTORY_NAME
    tdp_vars_defaults = directory / DEFAULT_VARS_DIRECTORY_NAME
    tdp_vars_schema = directory / SCHEMA_VARS_DIRECTORY_NAME

    tdp_lib_dag.mkdir()
    playbooks.mkdir()
    tdp_vars_defaults.mkdir()
    tdp_vars_schema.mkdir()

    for service, operations in dag_service_operations.items():
        service_filename = service + ".yml"
        with (tdp_lib_dag / service_filename).open("w") as fd:
            yaml.dump(operations, fd)

        for operation in operations:
            if operation["name"].endswith("_start"):
                with (
                    playbooks / (operation["name"].rstrip("_start") + "_restart.yml")
                ).open("w") as fd:
                    pass
            with (playbooks / (operation["name"] + ".yml")).open("w") as fd:
                pass

    for service_name, file_vars in service_vars.items():
        service_dir = tdp_vars_defaults / service_name
        service_dir.mkdir()
        for filename, vars in file_vars.items():
            with (service_dir / filename).open("w") as fd:
                yaml.dump(vars, fd)

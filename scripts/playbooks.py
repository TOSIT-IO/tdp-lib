# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

"""
Generate meta playbooks in order to use a TDP like collection without tdp-lib.

It checks the ansible playbooks which are given as input and returns a meta folder with the following yaml files:

- all_per_service.yml listing all service.
- all.yml listing the location of all yaml files of every service.
- yaml file for each specified service containing the location of every yaml file concerning this service.

The command `python path/to/playbooks.py` will execute the script with all collections defined in the `TDP_COLLECTION_PATH` including every service.

Use the `-h` or `--help` option to get further information about the options.
"""

from pathlib import Path

import click
import networkx as nx

from tdp.cli.params import collections_option
from tdp.core.constants import DEFAULT_SERVICE_PRIORITY, SERVICE_PRIORITY
from tdp.core.dag import Dag
from tdp.core.entities.operation import OperationName


@click.command()
@click.argument("services", required=False, nargs=-1)
@click.option(
    "--output-dir",
    type=Path,
    help="Output directory where playbooks will be written.",
    required=False,
    default=".",
)
@click.option(
    "--for-collection",
    type=str,
    help="Only write operations from this collection.",
    required=False,
    multiple=True,
)
@collections_option
def playbooks(services, output_dir, for_collection, collections):
    """Generate meta playbooks in order to use a TDP like collection without tdp-lib."""
    dag = Dag(collections)
    # services DAG
    dag_services = nx.DiGraph()
    # For each service, get all operations with DAG topological_sort order
    dag_service_operations = {}
    for operation in dag.get_all_operations():
        dag_services.add_node(operation.name.service)
        for dependency in operation.depends_on:
            dependency_operation = OperationName.from_str(dependency)
            if dependency_operation.service != operation.name.service:
                dag_services.add_edge(
                    dependency_operation.service, operation.name.service
                )
        dag_service_operations.setdefault(operation.name.service, []).append(operation)

    if not nx.is_directed_acyclic_graph(dag_services):
        raise RuntimeError("dag_services is not a DAG.")

    def custom_key(node):
        operation_priority = SERVICE_PRIORITY.get(node, DEFAULT_SERVICE_PRIORITY)
        return f"{operation_priority:02d}_{node}"

    dag_services_order = nx.lexicographical_topological_sort(dag_services, custom_key)

    if services:
        services = set(services)

        for service in services:
            if service not in dag_services.nodes:
                raise ValueError(
                    f"Service '{service}' is not in the DAG, services available: {dag_services.nodes}."
                )
        # Reorder services specified with services DAG topological_sort order
        services = [
            dag_service for dag_service in dag_services_order if dag_service in services
        ]
    else:
        services = dag_services_order

    meta_dir = Path(output_dir, "meta")
    meta_dir.mkdir()

    def write_copyright_licence_headers(fd):
        fd.write("# Copyright 2022 TOSIT.IO\n")
        fd.write("# SPDX-License-Identifier: Apache-2.0\n\n")

    playbooks_prefix = "../"
    with Path(meta_dir, "all_per_service.yml").open("w") as all_per_service_fd:
        write_copyright_licence_headers(all_per_service_fd)
        all_per_service_fd.write("---\n")
        is_noop = lambda op: op.noop
        is_in_collection = lambda op: op.collection_name in for_collection
        for service in services:
            if for_collection and not any(
                map(is_in_collection, dag_service_operations[service])
            ):
                continue
            if all(map(is_noop, dag_service_operations[service])):
                all_per_service_fd.write(f"# {service}\n")
                continue
            all_per_service_fd.write(
                f"- ansible.builtin.import_playbook: {service}.yml\n"
            )
            with Path(meta_dir, f"{service}.yml").open("w") as service_fd:
                write_copyright_licence_headers(service_fd)
                service_fd.write("---\n")
                for operation in dag_service_operations[service]:
                    if (
                        for_collection
                        and operation.collection_name not in for_collection
                    ):
                        continue
                    if not operation.noop:
                        service_fd.write(
                            f"- ansible.builtin.import_playbook: {playbooks_prefix}{operation.name}.yml\n"
                        )
                    else:
                        service_fd.write(f"# {operation.name}\n")

    with Path(meta_dir, "all.yml").open("w") as all_fd:
        write_copyright_licence_headers(all_fd)
        all_fd.write("---\n")
        for operation in dag.get_all_operations():
            if for_collection and operation.collection_name not in for_collection:
                continue
            if not operation.noop:
                all_fd.write(
                    f"- ansible.builtin.import_playbook: {playbooks_prefix}{operation.name}.yml\n"
                )
            else:
                all_fd.write(f"# {operation.name}\n")


if __name__ == "__main__":
    playbooks()

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import click
import networkx as nx

from tdp.cli.utils import collection_paths
from tdp.core.dag import DEFAULT_SERVICE_PRIORITY, SERVICE_PRIORITY, Dag
from tdp.core.operation import Operation


@click.command(
    short_help="Generate meta playbooks in order to use tdp-collection without tdp-lib"
)
@click.argument("services", required=False, nargs=-1)
@click.option(
    "--collection-path",
    envvar="TDP_COLLECTION_PATH",
    required=True,
    callback=collection_paths,  # transforms list of path into list of Collection
    help=f"List of paths separated by your os' path separator ({os.pathsep})",
)
@click.option(
    "--output-dir",
    type=Path,
    help="Output dir where playbooks will be written",
    required=False,
    default=".",
)
def playbooks(services, collection_path, output_dir):
    dag = Dag.from_collections(collection_path)
    # services DAG
    dag_services = nx.DiGraph()
    # For each service, get all operations with DAG topological_sort order
    dag_service_operations = {}
    for operation_name in dag.get_all_operations():
        operation = dag.operations[operation_name]
        dag_services.add_node(operation.service)
        for dependency in operation.depends_on:
            dependency_operation = Operation(dependency)
            if dependency_operation.service != operation.service:
                dag_services.add_edge(dependency_operation.service, operation.service)
        dag_service_operations.setdefault(operation.service, []).append(operation)

    if not nx.is_directed_acyclic_graph(dag_services):
        raise RuntimeError("dag_services is not a DAG")

    def custom_key(node):
        operation_priority = SERVICE_PRIORITY.get(node, DEFAULT_SERVICE_PRIORITY)
        return f"{operation_priority:02d}_{node}"

    dag_services_order = nx.lexicographical_topological_sort(dag_services, custom_key)

    if services:
        services = set(services)

        for service in services:
            if service not in dag_services.nodes:
                raise ValueError(
                    f"Service '{service}' is not in the DAG, services available: {dag_services.nodes}"
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
        for service in services:
            all_per_service_fd.write(f"- import_playbook: {service}.yml\n")
            with Path(meta_dir, f"{service}.yml").open("w") as service_fd:
                write_copyright_licence_headers(service_fd)
                service_fd.write("---\n")
                for operation in dag_service_operations[service]:
                    if not operation.noop:
                        service_fd.write(
                            f"- import_playbook: {playbooks_prefix}{operation.name}.yml\n"
                        )
                    else:
                        service_fd.write(f"# {operation.name}\n")

    with Path(meta_dir, "all.yml").open("w") as all_fd:
        write_copyright_licence_headers(all_fd)
        all_fd.write("---\n")
        for operation_name in dag.get_all_operations():
            operation = dag.operations[operation_name]
            if not operation.noop:
                all_fd.write(
                    f"- import_playbook: {playbooks_prefix}{operation_name}.yml\n"
                )
            else:
                all_fd.write(f"# {operation_name}\n")

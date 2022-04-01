# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import click

from tdp.cli.context import pass_dag
from tdp.cli.session import get_session_class
from tdp.core.runner.action_runner import ActionRunner
from tdp.core.runner.ansible_executor import AnsibleExecutor
from tdp.core.service_manager import ServiceManager


@click.command(short_help="Deploy TDP")
@click.argument("target", required=False)
@click.option(
    "--sqlite-path",
    envvar="TDP_SQLITE_PATH",
    type=Path,
    help="Path to SQLITE database file",
    required=True,
)
@click.option(
    "--collection-path",
    envvar="TDP_COLLECTION_PATH",
    type=Path,
    help="Path to tdp-collection",
    required=True,
)
@click.option(
    "--run-directory",
    envvar="TDP_RUN_DIRECTORY",
    type=Path,
    help="Working directory where the executor is launched (`ansible-playbook` for Ansible)",
    required=True,
)
@click.option(
    "--vars", envvar="TDP_VARS", type=Path, help="Path to the tdp vars", required=True
)
@click.option("--filter", type=str, help="Glob on list name")
@click.option("--dry", is_flag=True, help="Execute dag without running any action")
@pass_dag
def deploy(dag, target, sqlite_path, collection_path, run_directory, vars, filter, dry):
    if target and target not in dag.components:
        raise ValueError(f"{target} is not a valid node")
    playbooks_directory = collection_path / "playbooks"
    run_directory = run_directory.absolute() if run_directory else None

    ansible_executor = AnsibleExecutor(
        playbooks_directory=playbooks_directory,
        run_directory=run_directory,
        dry=dry,
    )
    session_class = get_session_class(sqlite_path)
    with session_class() as session:
        service_managers = ServiceManager.get_service_managers(dag, vars)
        check_services_cleanliness(service_managers)

        action_runner = ActionRunner(dag, ansible_executor, service_managers)
        if target:
            click.echo(f"Deploying {target}")
        else:
            click.echo(f"Deploying TDP")
        deployment = action_runner.run_to_node(target, filter)
        session.add(deployment)
        session.commit()


def check_services_cleanliness(service_managers):
    unclean_services = [
        service_manager.name
        for service_manager in service_managers.values()
        if not service_manager.clean
    ]
    if unclean_services:
        for name in unclean_services:
            click.echo(f"{name} repository is not in a clean state")
        raise ValueError("Some services are in a dirty state")

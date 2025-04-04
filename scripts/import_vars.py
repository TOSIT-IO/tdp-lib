# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

"""Import variables from a directory into an existing tdp_vars directory.

Imported services must follow the same structure as the tdp_vars directory.
COMMIT_EDITMSG file must be present in the imported directory in order to provide a
commit message.
"""

import logging
from pathlib import Path

import click
from sqlalchemy import Engine

from tdp.cli.params import collections_option, database_dsn_option
from tdp.cli.params.vars_option import vars_option
from tdp.core.collections import Collections
from tdp.core.repository.git_repository import GitRepository
from tdp.core.repository.repository import EmptyCommit, NotARepository
from tdp.core.variables.cluster_variables import ClusterVariables
from tdp.core.variables.service_variables import ServiceVariables
from tdp.dao import Dao

logger = logging.getLogger(__name__)

COMMIT_MESSAGE_FILE = "COMMIT_EDITMSG"


class ServiceUpdater:
    service_variables: ServiceVariables
    commit_message: str
    import_path: Path

    def __init__(self, import_path: Path, service_path: Path):
        self.import_path = import_path
        self.commit_message = self._get_commit_message(import_path)
        self.service_variables = self._get_service_variables(service_path)

    def _get_service_variables(self, service_path: Path):
        try:
            service_repo = GitRepository(service_path)
        except NotARepository:
            raise ValueError(f"{service_path.absolute()} is not a repository")

        return ServiceVariables(service_repo, None)

    def _get_commit_message(self, import_path: Path):
        commit_message_file = import_path / COMMIT_MESSAGE_FILE
        if commit_message_file.exists():
            with commit_message_file.open() as f:
                return f.read()
        else:
            raise ValueError(f"{commit_message_file} does not exist")

    def update(self):
        self.service_variables.update_from_dir(
            self.import_path,
            validation_message=self.commit_message,
        )


@click.command()
@vars_option
@database_dsn_option
@collections_option
@click.argument(
    "directory",
    type=click.Path(
        path_type=Path, exists=True, file_okay=False, dir_okay=True, resolve_path=True
    ),
    nargs=1,
)
def import_vars(
    directory: Path, vars: Path, db_engine: Engine, collections: Collections
):
    """Import variables from a directory."""

    click.echo(f"Importing variables from {directory}.")
    services_to_update: list[ServiceUpdater] = []
    errors = []

    for path in directory.iterdir():
        # Skip files
        if not path.is_dir():
            continue

        service_name = path.name
        service_path = vars / service_name

        try:
            services_to_update.append(ServiceUpdater(path, service_path))
        except ValueError as e:
            errors.append(e)

    if errors:
        click.echo("Errors occurred:")
        for error in errors:
            click.echo(error)
        click.echo("No variables have been imported.")
        return

    for service_updater in services_to_update:
        try:
            service_updater.update()
            click.echo(
                f"Variables for {service_updater.service_variables.name} have been updated"
            )
            click.echo(f"Commit message: {service_updater.commit_message}")
        except EmptyCommit:
            click.echo(
                f"Override file {service_updater.service_variables.path.absolute()} will not cause any change, no commit has been made"
            )

    # Generate stale component list and save it to the database
    with Dao(db_engine) as dao:
        stale_status_logs = dao.get_cluster_status().generate_stale_sch_logs(
            cluster_variables=ClusterVariables.get_cluster_variables(collections, vars),
            collections=collections,
        )
        dao.session.add_all(stale_status_logs)
        dao.session.commit()


if __name__ == "__main__":
    import_vars()

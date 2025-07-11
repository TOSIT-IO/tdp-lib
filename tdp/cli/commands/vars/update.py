# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING

import click

from tdp.cli.params.collections_option import collections_option
from tdp.cli.params.conf_option import conf_option
from tdp.cli.params.database_dsn_option import database_dsn_option
from tdp.cli.params.validate_option import validate_option
from tdp.cli.params.vars_option import vars_option
from tdp.core.constants import DEFAULT_VALIDATION_MESSAGE, VALIDATION_MESSAGE_FILE

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from tdp.core.collections import Collections

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--validation-message",
    "-m",
    "msg",
    help="Message to be used as a validation message for all services.",
    default=DEFAULT_VALIDATION_MESSAGE,
)
@click.option(
    "--validation-message-file",
    "-M",
    "msg_file",
    help="Name of the file containing a custom validation message to read in each imported service. When multiple validations message are provided (default --validation-message and/or service imported from multiple import directories), they will be concatenated.",
    default=VALIDATION_MESSAGE_FILE,
)
@conf_option
@vars_option
@database_dsn_option
@collections_option
@validate_option
def update(
    conf: tuple[pathlib.Path],
    vars: pathlib.Path,
    db_engine: Engine,
    collections: Collections,
    validate: bool,
    msg: str,
    msg_file: str,
):
    """Update configuration from the given directories."""

    from tdp.core.variables.cluster_variables import (
        ClusterVariables,
        ServicesNotInitializedError,
    )
    from tdp.core.variables.exceptions import ServicesUpdateError
    from tdp.dao import Dao

    cluster_variables = ClusterVariables.get_cluster_variables(collections, vars)
    try:
        cluster_variables.update(
            conf,
            validate=validate,
            validation_msg_file_name=msg_file,
            base_validation_msg=msg,
        )
    # Stop the update process if some services are not initialized
    except ServicesNotInitializedError as e:
        error_messages = "\n".join(
            f"{error.service_name} (from {error.source_definition})"
            for error in e.services
        )
        raise click.ClickException(
            f"The following services are not initialized:\n{error_messages}"
        ) from e
    # Do not stop the update as some services may have been updated successfully
    except ServicesUpdateError as e:
        error_messages = "\n".join(
            f"{error.service_name}: {error.message}" for error in e.errors
        )
        logger.error(
            f"Errors occurred during service updates:\n{error_messages}",
            exc_info=True,
        )
    except Exception:
        logger.error("Unexpeced error", exc_info=True)

    # Generate stale component list and save it to the database
    with Dao(db_engine) as dao:
        stale_status_logs = dao.get_cluster_status().generate_stale_sch_logs(
            cluster_variables=cluster_variables,
            collections=collections,
        )
        dao.session.add_all(stale_status_logs)
        dao.session.commit()

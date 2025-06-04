# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
import pathlib

import click
from sqlalchemy import Engine

from tdp.cli.params.collections_option import collections_option
from tdp.cli.params.database_dsn_option import database_dsn_option
from tdp.cli.params.overrides_option import overrides_option
from tdp.cli.params.validate_option import validate_option
from tdp.cli.params.vars_option import vars_option
from tdp.core.collections.collections import Collections
from tdp.core.variables.cluster_variables import (
    DEFAULT_VALIDATION_MESSAGE,
    VALIDATION_MESSAGE_FILE,
    ClusterVariables,
)
from tdp.dao import Dao

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
@overrides_option
@vars_option
@database_dsn_option
@collections_option
@validate_option
def update(
    overrides: tuple[pathlib.Path],
    vars: pathlib.Path,
    db_engine: Engine,
    collections: Collections,
    validate: bool,
    msg: str,
    msg_file: str,
):
    """Update configuration from the given directories."""
    cluster_variables = ClusterVariables.get_cluster_variables(collections, vars)
    try:
        cluster_variables.update(
            overrides,
            validate=validate,
            validation_msg_file_name=msg_file,
            base_validation_msg=msg,
        )
    except Exception as e:
        logger.error("Error while updating cluster variables: %s", e)
        raise

    # Generate stale component list and save it to the database
    with Dao(db_engine) as dao:
        stale_status_logs = dao.get_cluster_status().generate_stale_sch_logs(
            cluster_variables=cluster_variables,
            collections=collections,
        )
        dao.session.add_all(stale_status_logs)
        dao.session.commit()

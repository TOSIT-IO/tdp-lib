# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import click
from sqlalchemy import Engine

from tdp.cli.params import database_dsn_option
from tdp.core.inventory_reader import InventoryReader
from tdp.core.models.sch_status_log_model import SCHStatusLogModel
from tdp.dao import Dao


@click.command()
@database_dsn_option
def update(db_engine: Engine) -> None:
    """Updates the `is_active` column of the sch_status_log if a host has been decommisioned."""

    host_names: list[str] = InventoryReader().get_hosts()
    click.echo("Current host names in inventory file: " + ", ".join(host_names))

    with Dao(db_engine) as dao:

        try:
            sch_status_list: list[SCHStatusLogModel] = (
                dao.get_hosted_entity_statuses_history()
            )
            for sch_status in sch_status_list:
                if sch_status.host is not None and sch_status.host not in host_names:
                    sch_status.is_active = False
            dao.session.commit()
            click.echo("sch_status_log table has successfully been updated")

        except:
            raise click.ClickException("sch_status_log table update has failed")

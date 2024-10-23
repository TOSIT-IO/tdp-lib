# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import click
from sqlalchemy import Engine

from tdp.cli.params import database_dsn_option
from tdp.core.entities.hosted_entity import HostedServiceComponent
from tdp.core.inventory_reader import InventoryReader
from tdp.core.models.sch_status_log_model import (
    SCHStatusLogModel,
    SCHStatusLogSourceEnum,
)
from tdp.dao import Dao


@click.command()
@database_dsn_option
def prune_hosts(db_engine: Engine) -> None:
    """Sets `is_active` to False for decommissioned host."""

    available_hosts: list[str] = InventoryReader().get_hosts()
    updated_statuses: list[SCHStatusLogModel] = []

    with Dao(db_engine) as dao:

        statuses = dao.get_hosted_entity_statuses()
        for status in statuses:
            if status.entity.host is None:
                continue
            if status.entity.host not in available_hosts:
                updated_statuses.append(
                    SCHStatusLogModel(
                        service=status.entity.name.service,
                        component=(
                            status.entity.name.component
                            if isinstance(status.entity, HostedServiceComponent)
                            else None
                        ),
                        host=status.entity.host,
                        source=SCHStatusLogSourceEnum.DECOMMISSION,
                        is_active=False,
                    )
                )
        if updated_statuses:
            dao.session.add_all(updated_statuses)
            dao.session.commit()
            click.echo(
                "Added inactive components with decommissioned host in the sch_status_log table."
            )
        else:
            click.echo("No new inactive components.")

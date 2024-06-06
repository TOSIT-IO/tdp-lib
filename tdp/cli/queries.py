# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Iterable
from operator import and_
from typing import TYPE_CHECKING, NamedTuple, Optional

from sqlalchemy import Select, case, func, or_, select
from sqlalchemy.exc import NoResultFound

from tdp.core.models import (
    DeploymentModel,
    SCHStatusLogModel,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


def _create_last_value_statement(column, non_null=False):
    """Create a windowed query that returns last value of a column.

    Args:
        column: The column to return the last value of.
        non_null: Whether to return the last non-null value.
    """
    order_by = SCHStatusLogModel.event_time.desc()
    if non_null:
        order_by = case((column == None, 0), else_=1).desc(), order_by
    return func.first_value(column).over(
        partition_by=(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
        ),
        order_by=order_by,
    )


class SCHLatestStatus(NamedTuple):
    service: str
    component: Optional[str]
    host: Optional[str]
    latest_running_version: Optional[str]
    latest_configured_version: Optional[str]
    latest_to_config: Optional[bool]
    latest_to_restart: Optional[bool]
    latest_is_active: Optional[bool]


def create_get_sch_latest_status_statement(
    service_to_filter: Optional[str] = None,
    component_to_filter: Optional[str] = None,
    hosts_to_filter: Optional[Iterable[str]] = None,
    filter_stale: Optional[bool] = None,
    filter_active: Optional[bool] = None,
) -> Select[SCHLatestStatus]:
    """Create a query to get the cluster status.

    Args:
        service_to_filter: The service to filter.
        component_to_filter: The component to filter.
        host_to_filter: The host to filter.
        filter_stale: Whether to filter stale status.
          True for stale, False for not stale, None for all.
    """
    subquery_filter = []
    if service_to_filter:
        subquery_filter.append(SCHStatusLogModel.service == service_to_filter)
    if component_to_filter:
        subquery_filter.append(SCHStatusLogModel.component == component_to_filter)
    if hosts_to_filter:
        subquery_filter.append(SCHStatusLogModel.host.in_(hosts_to_filter))

    subq = (
        select(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
            _create_last_value_statement(
                SCHStatusLogModel.running_version, non_null=True
            ).label("latest_running_version"),
            _create_last_value_statement(
                SCHStatusLogModel.configured_version, non_null=True
            ).label("latest_configured_version"),
            _create_last_value_statement(
                SCHStatusLogModel.to_config, non_null=True
            ).label("latest_to_config"),
            _create_last_value_statement(
                SCHStatusLogModel.to_restart, non_null=True
            ).label("latest_to_restart"),
            _create_last_value_statement(
                SCHStatusLogModel.is_active, non_null=True
            ).label("latest_is_active"),
        )
        .filter(*subquery_filter)
        .distinct()
        .subquery()
    )

    query_filter = []
    if filter_stale is True:
        query_filter.append(
            or_(
                subq.c.latest_to_config.is_(True),
                subq.c.latest_to_restart.is_(True),
            )
        )
    elif filter_stale is False:
        query_filter.append(
            and_(
                subq.c.latest_to_config.is_not(True),
                subq.c.latest_to_restart.is_not(True),
            )
        )

    if filter_active is True:
        query_filter.append(subq.c.latest_is_active.is_not(False))
    elif filter_active is False:
        query_filter.append(subq.c.latest_is_active.is_(False))

    return select(subq).filter(*query_filter)


def get_deployments(session: Session, limit: int, offset: int) -> list[DeploymentModel]:
    """Get deployments.

    Args:
        session: The database session.
        limit: The maximum number of deployments to return.
        offset: The offset at which to start the query.

    Returns:
        The deployments.
    """
    return (
        session.query(DeploymentModel)
        .order_by(DeploymentModel.id.desc())
        .limit(limit)
        .offset(offset)
        .all()
    )


def get_last_deployment(session: Session) -> DeploymentModel:
    """Get the last deployment.

    Args:
        session: The database session.

    Returns:
        The last deployment.

    Raises:
        NoResultFound: If there is no deployment.
    """
    try:
        return (
            session.query(DeploymentModel)
            .order_by(DeploymentModel.id.desc())
            .limit(1)
            .one()
        )
    except NoResultFound as e:
        raise Exception("No deployments.") from e

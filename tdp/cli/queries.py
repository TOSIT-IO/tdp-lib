# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from sqlalchemy import case, func
from sqlalchemy.exc import NoResultFound

from tdp.core.models import (
    DeploymentModel,
    OperationModel,
    SCHStatusLogModel,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from tdp.core.models import SCHStatusRow


def create_windowed_statement(column):
    """Create a windowed query.

    Args:
        column: The column to window.

    Returns:
        The windowed query.
    """
    return func.first_value(column).over(
        partition_by=(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
        ),
        order_by=(
            case((column == None, 0), else_=1).desc(),
            SCHStatusLogModel.event_time.desc(),
        ),
    )


def get_sch_status(
    session: Session,
) -> list[SCHStatusRow]:
    """Get cluster status.

    Recover the latest values for each (service, component, host) combination.

    Args:
        session: The database session.

    Returns:
        The cluster status.
    """
    base_columns = (
        SCHStatusLogModel.service,
        SCHStatusLogModel.component,
        SCHStatusLogModel.host,
    )
    queryable_columns = (
        SCHStatusLogModel.running_version,
        SCHStatusLogModel.configured_version,
        SCHStatusLogModel.to_config,
        SCHStatusLogModel.to_restart,
    )
    for column in queryable_columns:
        base_columns += (create_windowed_statement(column).label(column.name),)
    return (
        session.query(
            *base_columns,
        )
        .distinct()
        .all()
    )


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


def get_deployment(session: Session, deployment_id: int) -> DeploymentModel:
    """Get a deployment by ID.

    Args:
        session: The database session.
        deployment_id: The deployment ID.

    Returns:
        The deployment.

    Raises:
        NoResultFound: If the deployment does not exist."""
    try:
        return session.query(DeploymentModel).filter_by(id=deployment_id).one()
    except NoResultFound as e:
        raise Exception(f"Deployment with ID {deployment_id} does not exist.") from e


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


def get_planned_deployment(session: Session) -> Optional[DeploymentModel]:
    """Get the planned deployment.

    Args:
        session: The database session.

    Returns:
        The planned deployment or None if there is no planned deployment.
    """
    return session.query(DeploymentModel).filter_by(state="PLANNED").one_or_none()


def get_operation_records(
    session: Session, deployment_id: int, operation_name: str
) -> list[OperationModel]:
    """Get an operation records.

    Args:
        session: The database session.
        deployment_id: The deployment ID.
        operation_name: The operation name.

    Returns:
        List of matching operation records.

    Raises:
        NoResultFound: If the operation does not exist.
    """
    try:
        return (
            session.query(OperationModel)
            .filter_by(deployment_id=deployment_id, operation=operation_name)
            .all()
        )
    except NoResultFound as e:
        raise Exception(
            f"Operation {operation_name} does not exist in deployment {deployment_id}."
        ) from e

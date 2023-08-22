# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from sqlalchemy import and_, desc, func, or_, select, tuple_
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import joinedload

from tdp.core.models import (
    ComponentVersionLog,
    DeploymentLog,
    OperationLog,
    StaleComponent,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


def get_stale_components(session: Session) -> list[StaleComponent]:
    """Get stale components.

    Args:
        session: The database session.

    Returns:
        The stale components.
    """
    return session.query(StaleComponent).all()


def get_latest_success_component_version_log(
    session: Session,
) -> list[ComponentVersionLog]:
    """Get the latest success component version.

    Args:
        session: The database session.

    Returns:
        Components with the latest success version. ([id, service_name, component_name, short_version])
    """
    # A label to represent the maximum deployment ID for readability.
    max_deployment_id_label = f"max_{ComponentVersionLog.deployment_id.name}"

    # Subquery: Identify the latest successful deployment ID for each service,
    # component, and host combination.
    latest_deployed_component = (
        session.query(
            func.max(ComponentVersionLog.deployment_id).label(max_deployment_id_label),
            ComponentVersionLog.service,
            ComponentVersionLog.component,
            ComponentVersionLog.host,
        )
        .group_by(
            ComponentVersionLog.service,
            ComponentVersionLog.component,
            ComponentVersionLog.host,
        )
        .subquery()
    )

    # Main query: Retrieve ComponentVersionLog entries that match the latest
    # deployment IDs identified in the subquery.
    return (
        session.query(ComponentVersionLog)
        .filter(
            or_(
                # Components with the latest success deployment for each host
                tuple_(
                    ComponentVersionLog.deployment_id,
                    ComponentVersionLog.service,
                    ComponentVersionLog.component,
                    ComponentVersionLog.host,
                ).in_(select(latest_deployed_component)),
                # Services with the latest success deployment for each host when there's
                # no specific component
                and_(
                    tuple_(
                        ComponentVersionLog.deployment_id,
                        ComponentVersionLog.service,
                    ).in_(
                        select(
                            latest_deployed_component.c[max_deployment_id_label],
                            latest_deployed_component.c.service,
                        )
                    ),
                    ComponentVersionLog.component.is_(
                        None
                    ),  # Ensure the component is null for this condition
                ),
            )
        )
        .order_by(
            ComponentVersionLog.service,
            ComponentVersionLog.component,
            ComponentVersionLog.host,
            desc(ComponentVersionLog.deployment_id),
        )
        .all()
    )


def get_deployments(session: Session, limit: int, offset: int) -> list[DeploymentLog]:
    """Get deployments.

    Args:
        session: The database session.
        limit: The maximum number of deployments to return.
        offset: The offset at which to start the query.

    Returns:
        The deployments.
    """
    return (
        session.query(DeploymentLog)
        .options(joinedload(DeploymentLog.component_version))
        .order_by(DeploymentLog.id.desc())
        .limit(limit)
        .offset(offset)
        .all()
    )


def get_deployment(session: Session, deployment_id: int) -> DeploymentLog:
    """Get a deployment by id.

    Args:
        session: The database session.
        deployment_id: The deployment id.

    Returns:
        The deployment.

    Raises:
        NoResultFound: If the deployment does not exist."""
    try:
        return session.query(DeploymentLog).filter_by(id=deployment_id).one()
    except NoResultFound as e:
        raise Exception(f"Deployment id {deployment_id} does not exist.") from e


def get_last_deployment(session: Session) -> DeploymentLog:
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
            session.query(DeploymentLog)
            .order_by(DeploymentLog.id.desc())
            .limit(1)
            .one()
        )
    except NoResultFound as e:
        raise Exception(f"No deployments.") from e


def get_planned_deployment_log(session: Session) -> Optional[DeploymentLog]:
    """Get the planned deployment.

    Args:
        session: The database session.

    Returns:
        The planned deployment or None if there is no planned deployment.
    """
    return session.query(DeploymentLog).filter_by(state="PLANNED").one_or_none()


def get_operation_log(
    session: Session, deployment_id: int, operation_name: str
) -> OperationLog:
    """Get an operation log.

    Args:
        session: The database session.
        deployment_id: The deployment id.
        operation_name: The operation name.

    Returns:
        The operation log.

    Raises:
        NoResultFound: If the operation does not exist.
    """
    try:
        return (
            session.query(OperationLog)
            .filter_by(deployment_id=deployment_id, operation=operation_name)
            .one()
        )
    except NoResultFound as e:
        raise Exception(
            f"Operation {operation_name} does not exist in deployment {deployment_id}."
        ) from e

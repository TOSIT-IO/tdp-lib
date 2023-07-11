# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional, Tuple

from sqlalchemy import and_, desc, func, or_, select, tuple_
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session

from tdp.core.models import ComponentVersionLog, DeploymentLog, OperationLog


def get_latest_success_component_version_log(
    session: Session,
) -> List[Tuple[int, str, str, str]]:
    """Get the latest success component version.

    Args:
        session: The database session.

    Returns:
        Components with the latest success version. ([id, service_name, component_name, short_version])
    """
    max_deployment_id_label = f"max_{ComponentVersionLog.deployment_id.name}"

    # Latest success deployment for each service/component pair
    latest_deployed_component = (
        session.query(
            func.max(ComponentVersionLog.deployment_id).label(max_deployment_id_label),
            ComponentVersionLog.service,
            ComponentVersionLog.component,
        )
        .group_by(ComponentVersionLog.service, ComponentVersionLog.component)
        .subquery()
    )

    return (
        session.query(
            ComponentVersionLog.deployment_id,
            ComponentVersionLog.service,
            ComponentVersionLog.component,
            func.substr(ComponentVersionLog.version, 1, 7),
        )
        .filter(
            or_(
                # Components with the latest success deployment
                # Filter deployment_id, service and component from the subquery
                tuple_(
                    ComponentVersionLog.deployment_id,
                    ComponentVersionLog.service,
                    ComponentVersionLog.component,
                ).in_(select(latest_deployed_component)),
                # Services with the latest success depoyment (no component)
                # Filter deployment_id and service from the subquery AND component is null
                and_(
                    tuple_(
                        ComponentVersionLog.deployment_id,
                        ComponentVersionLog.service,
                    ).in_(
                        # Component column is removed from the subquery
                        select(
                            latest_deployed_component.c[max_deployment_id_label],
                            latest_deployed_component.c.service,
                        )
                    ),
                    ComponentVersionLog.component.is_(None),
                ),
            )
        )
        .order_by(
            desc(ComponentVersionLog.deployment_id),
            ComponentVersionLog.service,
            ComponentVersionLog.component,
        )
        .all()
    )


def get_deployments(session: Session, limit: int, offset: int) -> List[DeploymentLog]:
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

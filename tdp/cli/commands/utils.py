# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional
from sqlalchemy.orm import Session
from tdp.core.models import DeploymentLog, OperationLog, ServiceComponentLog
from tdp.cli.queries import (
    get_deployment_query,
    get_deployments_query,
    get_operation_log_query,
    get_last_deployment_query,
    get_latest_success_service_component_version_query,
    get_planned_deployments_query,
)


def execute_get_latest_success_service_component_version_query(
    session: Session,
) -> ServiceComponentLog:
    """Get the latest successful service component version.

    Returns:
        The latest successful service component version.

    Raises:
        ValueError: If the latest successful service component version does not exist.
    """
    latest_success_service_component_version = session.execute(
        get_latest_success_service_component_version_query()
    ).all()
    if latest_success_service_component_version is None:
        raise ValueError(
            "No successful service component version found in the database"
        )
    return latest_success_service_component_version


def execute_get_deployment_query(session: Session, deployment_id: int) -> DeploymentLog:
    """Get a deployment by its id.

    Args:
        session: The database session.
        deployment_id: The deployment id.

    Returns:
        The deployment.

    Raises:
        ValueError: If the deployment does not exist.
    """
    deployment = (
        session.execute(get_deployment_query(deployment_id))
        .unique()
        .scalar_one_or_none()
    )
    if deployment is None:
        raise ValueError(f"Deployment {deployment_id} does not exist")
    return deployment


def execute_get_deployments_query(
    session: Session, limit: int, offset: int
) -> Optional[List[DeploymentLog]]:
    """Get deployments.

    Args:
        session: The database session.
        limit: The maximum number of deployments to return.
        offset: The offset from which to start the query.

    Returns:
        The deployments.
    """
    return (
        session.execute(get_deployments_query(limit, offset))
        .unique()
        .scalars()
        .fetchall()
    )


def execute_get_operation_log_query(
    session: Session, deployment_id: int, operation: str
) -> OperationLog:
    """Get an operation log by its deployment id and operation name.

    Args:
        session: The database session.
        deployment_id: The deployment id.
        operation: The operation name.

    Returns:
        The operation log.

    Raises:
        ValueError: If the operation does not exist.
    """
    operation_log = (
        session.execute(get_operation_log_query(deployment_id, operation))
        .unique()
        .scalar_one_or_none()
    )
    if operation_log is None:
        raise ValueError(
            f"Operation {operation} does not exist in deployment {deployment_id}"
        )
    return operation_log


def execute_get_last_deployment_query(session: Session) -> DeploymentLog:
    """Get the last deployment.

    Args:
        session: The database session.

    Returns:
        The last deployment.

    Raises:
        ValueError: If the deployment does not exist.
    """
    deployment = (
        session.execute(get_last_deployment_query()).unique().scalar_one_or_none()
    )
    if deployment is None:
        raise ValueError(f"No deployments found")
    return deployment


def execute_get_planned_deployment_query(session: Session) -> DeploymentLog:
    """Get a planned deployment.

    Args:
        session: The database session.

    Returns:
        A planned deployment or none.
    """
    deployments = session.execute(get_planned_deployments_query()).scalars().all()
    if len(deployments) > 1:
        raise ValueError(f"More than one planned deployment found")
    if len(deployments) == 1:
        return deployments[0]
    else:
        return None

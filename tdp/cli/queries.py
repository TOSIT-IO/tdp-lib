# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import and_, desc, func, or_, select, tuple_
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.expression import Executable
from tdp.core.models import DeploymentLog, ServiceComponentLog, OperationLog


def get_latest_success_service_component_version_query() -> Executable:
    """Returns a query that returns the latest successful deployment id for each service and component."""
    max_depid_label = f"max_{ServiceComponentLog.deployment_id.name}"

    latest_success_for_each_service = select(
        func.max(ServiceComponentLog.deployment_id).label(max_depid_label),
        ServiceComponentLog.service,
        ServiceComponentLog.component,
    ).group_by(ServiceComponentLog.service, ServiceComponentLog.component)
    # Request with or_ because querying with a tuple of 3 attributes using in_ operator
    # does not work when the value can be null (because NULL in_ NULL is translated to `NULL = NULL` which returns NULL)
    return (
        select(
            ServiceComponentLog.deployment_id,
            ServiceComponentLog.service,
            ServiceComponentLog.component,
            func.substr(ServiceComponentLog.version, 1, 7),
        )
        .filter(
            or_(
                tuple_(
                    ServiceComponentLog.deployment_id,
                    ServiceComponentLog.service,
                    ServiceComponentLog.component,
                ).in_(latest_success_for_each_service),
                and_(
                    tuple_(
                        ServiceComponentLog.deployment_id,
                        ServiceComponentLog.service,
                    ).in_(
                        select(
                            latest_success_for_each_service.c[0],
                            latest_success_for_each_service.c[1],
                        )
                    ),
                    ServiceComponentLog.component.is_(None),
                ),
            )
        )
        .order_by(
            desc(ServiceComponentLog.deployment_id),
            ServiceComponentLog.service,
            ServiceComponentLog.component,
        )
    )


def get_deployment_query(deployment_id: int) -> Executable:
    """Get a deployment by its id.

    Args:
        deployment_id: The id of the deployment to get.

    Returns:
        The query to get the deployment.
    """
    return (
        select(DeploymentLog)
        .options(
            joinedload(DeploymentLog.service_components),
            joinedload(DeploymentLog.operations),
        )
        .where(DeploymentLog.id == deployment_id)
        .order_by(DeploymentLog.id)
    )


def get_deployments_query(limit: int, offset: int) -> Executable:
    """Get a list of deployments.

    Args:
        limit: The maximum number of deployments to return.
        offset: The offset at which to start the query.

    Returns:
        The query to get the deployments.
    """
    return (
        select(DeploymentLog)
        .options(joinedload(DeploymentLog.service_components))
        .order_by(DeploymentLog.id)
        .limit(limit)
        .offset(offset)
    )


def get_last_deployment_query() -> Executable:
    """Get the last deployment.

    Returns:
        The query to get the last deployment.
    """
    return (
        select(DeploymentLog)
        .options(
            joinedload(DeploymentLog.service_components),
            joinedload(DeploymentLog.operations),
        )
        .order_by(DeploymentLog.id.desc())
        .limit(1)
    )


def get_operation_log_query(deployment_id: int, operation_name: str) -> Executable:
    """Get an operation log by its deployment id and operation name.

    Args:
        deployment_id: The id of the deployment.
        operation_name: The name of the operation.

    Returns:
        The query to get the operation log.
    """
    return (
        select(OperationLog)
        .options(
            joinedload(OperationLog.deployment),
            joinedload("deployment.service_components"),
        )
        .where(
            and_(
                OperationLog.deployment_id == deployment_id,
                OperationLog.operation == operation_name,
            )
        )
        .order_by(OperationLog.start_time)
    )

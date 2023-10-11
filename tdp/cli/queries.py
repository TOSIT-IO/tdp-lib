# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from sqlalchemy import and_, case, func, or_
from sqlalchemy.exc import NoResultFound

from tdp.core.models import (
    DeploymentLog,
    OperationLog,
    SCHStatusLog,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from tdp.core.models import SCHStatusRow


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
    # Get the latest timestamps of non null values for each (service, component, host)
    # combination.
    latest_configured_version_timestamp_subquery = (
        session.query(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
            func.max(
                case(
                    (
                        SCHStatusLog.configured_version != None,
                        SCHStatusLog.event_time,
                    )
                )
            ).label("latest_configured_version_timestamp"),
        )
        .group_by(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
        )
        .subquery()
    )

    latest_running_version_timestamp_subquery = (
        session.query(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
            func.max(
                case(
                    (
                        SCHStatusLog.running_version != None,
                        SCHStatusLog.event_time,
                    )
                )
            ).label("latest_running_version_timestamp"),
        )
        .group_by(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
        )
        .subquery()
    )

    latest_to_config_timestamp_subquery = (
        session.query(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
            func.max(
                case((SCHStatusLog.to_config != None, SCHStatusLog.event_time))
            ).label("latest_to_config_timestamp"),
        )
        .group_by(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
        )
        .subquery()
    )

    latest_to_restart_timestamp_subquery = (
        session.query(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
            func.max(
                case((SCHStatusLog.to_restart != None, SCHStatusLog.event_time))
            ).label("latest_to_restart_timestamp"),
        )
        .group_by(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
        )
        .subquery()
    )

    # Get the latest values for each (service, component, host) combination.
    latest_configured_version_value_subquery = (
        session.query(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
            SCHStatusLog.configured_version,
            SCHStatusLog.event_time,
        )
        .join(
            latest_configured_version_timestamp_subquery,
            and_(
                SCHStatusLog.service
                == latest_configured_version_timestamp_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLog.component == None,
                    SCHStatusLog.component
                    == latest_configured_version_timestamp_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLog.host == None,
                    SCHStatusLog.host
                    == latest_configured_version_timestamp_subquery.c.host,
                ),
                # Join based on matching timestamps for all the columns in the subquery.
                SCHStatusLog.event_time
                == latest_configured_version_timestamp_subquery.c.latest_configured_version_timestamp,
            ),
        )
        .subquery()
    )

    latest_running_version_value_subquery = (
        session.query(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
            SCHStatusLog.running_version,
            SCHStatusLog.event_time,
        )
        .join(
            latest_running_version_timestamp_subquery,
            and_(
                SCHStatusLog.service
                == latest_running_version_timestamp_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLog.component == None,
                    SCHStatusLog.component
                    == latest_running_version_timestamp_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLog.host == None,
                    SCHStatusLog.host
                    == latest_running_version_timestamp_subquery.c.host,
                ),
                # Join based on matching timestamps for all the columns in the subquery.
                SCHStatusLog.event_time
                == latest_running_version_timestamp_subquery.c.latest_running_version_timestamp,
            ),
        )
        .subquery()
    )

    latest_to_config_value_subquery = (
        session.query(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
            SCHStatusLog.to_config,
            SCHStatusLog.event_time,
        )
        .join(
            latest_to_config_timestamp_subquery,
            and_(
                SCHStatusLog.service == latest_to_config_timestamp_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLog.component == None,
                    SCHStatusLog.component
                    == latest_to_config_timestamp_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLog.host == None,
                    SCHStatusLog.host == latest_to_config_timestamp_subquery.c.host,
                ),
                # Join based on matching timestamps for all the columns in the subquery.
                SCHStatusLog.event_time
                == latest_to_config_timestamp_subquery.c.latest_to_config_timestamp,
            ),
        )
        .subquery()
    )

    latest_to_restart_value_subquery = (
        session.query(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
            SCHStatusLog.to_restart,
            SCHStatusLog.event_time,
        )
        .join(
            latest_to_restart_timestamp_subquery,
            and_(
                SCHStatusLog.service == latest_to_restart_timestamp_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLog.component == None,
                    SCHStatusLog.component
                    == latest_to_restart_timestamp_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLog.host == None,
                    SCHStatusLog.host == latest_to_restart_timestamp_subquery.c.host,
                ),
                # Join based on matching timestamps for all the columns in the subquery.
                SCHStatusLog.event_time
                == latest_to_restart_timestamp_subquery.c.latest_to_restart_timestamp,
            ),
        )
        .subquery()
    )

    return (
        session.query(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
            func.max(latest_running_version_value_subquery.c.running_version),
            func.max(latest_configured_version_value_subquery.c.configured_version),
            func.max(latest_to_config_value_subquery.c.to_config),
            func.max(latest_to_restart_value_subquery.c.to_restart),
        )
        .outerjoin(
            latest_running_version_value_subquery,
            and_(
                SCHStatusLog.service == latest_running_version_value_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLog.component == None,
                    SCHStatusLog.component
                    == latest_running_version_value_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLog.host == None,
                    SCHStatusLog.host == latest_running_version_value_subquery.c.host,
                ),
                SCHStatusLog.event_time
                == latest_running_version_value_subquery.c.event_time,
            ),
        )
        .outerjoin(
            latest_configured_version_value_subquery,
            and_(
                SCHStatusLog.service
                == latest_configured_version_value_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLog.component == None,
                    SCHStatusLog.component
                    == latest_configured_version_value_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLog.host == None,
                    SCHStatusLog.host
                    == latest_configured_version_value_subquery.c.host,
                ),
                SCHStatusLog.event_time
                == latest_configured_version_value_subquery.c.event_time,
            ),
        )
        .outerjoin(
            latest_to_config_value_subquery,
            and_(
                SCHStatusLog.service == latest_to_config_value_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLog.component == None,
                    SCHStatusLog.component
                    == latest_to_config_value_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLog.host == None,
                    SCHStatusLog.host == latest_to_config_value_subquery.c.host,
                ),
                SCHStatusLog.event_time == latest_to_config_value_subquery.c.event_time,
            ),
        )
        .outerjoin(
            latest_to_restart_value_subquery,
            and_(
                SCHStatusLog.service == latest_to_restart_value_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLog.component == None,
                    SCHStatusLog.component
                    == latest_to_restart_value_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLog.host == None,
                    SCHStatusLog.host == latest_to_restart_value_subquery.c.host,
                ),
                SCHStatusLog.event_time
                == latest_to_restart_value_subquery.c.event_time,
            ),
        )
        .group_by(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
        )
        .order_by(
            SCHStatusLog.service,
            SCHStatusLog.component,
            SCHStatusLog.host,
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
        raise Exception("No deployments.") from e


def get_planned_deployment_log(session: Session) -> Optional[DeploymentLog]:
    """Get the planned deployment.

    Args:
        session: The database session.

    Returns:
        The planned deployment or None if there is no planned deployment.
    """
    return session.query(DeploymentLog).filter_by(status="PLANNED").one_or_none()


def get_operation_log(
    session: Session, deployment_id: int, operation_name: str
) -> list[OperationLog]:
    """Get an operation log.

    Args:
        session: The database session.
        deployment_id: The deployment id.
        operation_name: The operation name.

    Returns:
        List of matching operation logs.

    Raises:
        NoResultFound: If the operation does not exist.
    """
    try:
        return (
            session.query(OperationLog)
            .filter_by(deployment_id=deployment_id, operation=operation_name)
            .all()
        )
    except NoResultFound as e:
        raise Exception(
            f"Operation {operation_name} does not exist in deployment {deployment_id}."
        ) from e

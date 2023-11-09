# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from sqlalchemy import and_, case, func, or_
from sqlalchemy.exc import NoResultFound

from tdp.core.models import (
    DeploymentModel,
    OperationModel,
    SCHStatusLogModel,
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
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
            func.max(
                case(
                    (
                        SCHStatusLogModel.configured_version != None,
                        SCHStatusLogModel.event_time,
                    )
                )
            ).label("latest_configured_version_timestamp"),
        )
        .group_by(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
        )
        .subquery()
    )

    latest_running_version_timestamp_subquery = (
        session.query(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
            func.max(
                case(
                    (
                        SCHStatusLogModel.running_version != None,
                        SCHStatusLogModel.event_time,
                    )
                )
            ).label("latest_running_version_timestamp"),
        )
        .group_by(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
        )
        .subquery()
    )

    latest_to_config_timestamp_subquery = (
        session.query(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
            func.max(
                case(
                    (SCHStatusLogModel.to_config != None, SCHStatusLogModel.event_time)
                )
            ).label("latest_to_config_timestamp"),
        )
        .group_by(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
        )
        .subquery()
    )

    latest_to_restart_timestamp_subquery = (
        session.query(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
            func.max(
                case(
                    (SCHStatusLogModel.to_restart != None, SCHStatusLogModel.event_time)
                )
            ).label("latest_to_restart_timestamp"),
        )
        .group_by(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
        )
        .subquery()
    )

    # Get the latest values for each (service, component, host) combination.
    latest_configured_version_value_subquery = (
        session.query(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
            SCHStatusLogModel.configured_version,
            SCHStatusLogModel.event_time,
        )
        .join(
            latest_configured_version_timestamp_subquery,
            and_(
                SCHStatusLogModel.service
                == latest_configured_version_timestamp_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLogModel.component == None,
                    SCHStatusLogModel.component
                    == latest_configured_version_timestamp_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLogModel.host == None,
                    SCHStatusLogModel.host
                    == latest_configured_version_timestamp_subquery.c.host,
                ),
                # Join based on matching timestamps for all the columns in the subquery.
                SCHStatusLogModel.event_time
                == latest_configured_version_timestamp_subquery.c.latest_configured_version_timestamp,
            ),
        )
        .subquery()
    )

    latest_running_version_value_subquery = (
        session.query(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
            SCHStatusLogModel.running_version,
            SCHStatusLogModel.event_time,
        )
        .join(
            latest_running_version_timestamp_subquery,
            and_(
                SCHStatusLogModel.service
                == latest_running_version_timestamp_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLogModel.component == None,
                    SCHStatusLogModel.component
                    == latest_running_version_timestamp_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLogModel.host == None,
                    SCHStatusLogModel.host
                    == latest_running_version_timestamp_subquery.c.host,
                ),
                # Join based on matching timestamps for all the columns in the subquery.
                SCHStatusLogModel.event_time
                == latest_running_version_timestamp_subquery.c.latest_running_version_timestamp,
            ),
        )
        .subquery()
    )

    latest_to_config_value_subquery = (
        session.query(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
            SCHStatusLogModel.to_config,
            SCHStatusLogModel.event_time,
        )
        .join(
            latest_to_config_timestamp_subquery,
            and_(
                SCHStatusLogModel.service
                == latest_to_config_timestamp_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLogModel.component == None,
                    SCHStatusLogModel.component
                    == latest_to_config_timestamp_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLogModel.host == None,
                    SCHStatusLogModel.host
                    == latest_to_config_timestamp_subquery.c.host,
                ),
                # Join based on matching timestamps for all the columns in the subquery.
                SCHStatusLogModel.event_time
                == latest_to_config_timestamp_subquery.c.latest_to_config_timestamp,
            ),
        )
        .subquery()
    )

    latest_to_restart_value_subquery = (
        session.query(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
            SCHStatusLogModel.to_restart,
            SCHStatusLogModel.event_time,
        )
        .join(
            latest_to_restart_timestamp_subquery,
            and_(
                SCHStatusLogModel.service
                == latest_to_restart_timestamp_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLogModel.component == None,
                    SCHStatusLogModel.component
                    == latest_to_restart_timestamp_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLogModel.host == None,
                    SCHStatusLogModel.host
                    == latest_to_restart_timestamp_subquery.c.host,
                ),
                # Join based on matching timestamps for all the columns in the subquery.
                SCHStatusLogModel.event_time
                == latest_to_restart_timestamp_subquery.c.latest_to_restart_timestamp,
            ),
        )
        .subquery()
    )

    # Individual query components
    max_running_version = func.max(
        latest_running_version_value_subquery.c.running_version
    )
    max_configured_version = func.max(
        latest_configured_version_value_subquery.c.configured_version
    )

    bool_map = {True: 1, False: 0}
    case_to_config = case(
        bool_map, value=latest_to_config_value_subquery.c.to_config, else_=0
    )

    case_to_restart = case(
        bool_map, value=latest_to_restart_value_subquery.c.to_restart, else_=0
    )

    max_to_config = func.max(case_to_config).label("max_to_config")
    max_to_restart = func.max(case_to_restart).label("max_to_restart")

    return (
        session.query(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
            max_running_version,
            max_configured_version,
            max_to_config,
            max_to_restart,
        )
        .outerjoin(
            latest_running_version_value_subquery,
            and_(
                SCHStatusLogModel.service
                == latest_running_version_value_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLogModel.component == None,
                    SCHStatusLogModel.component
                    == latest_running_version_value_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLogModel.host == None,
                    SCHStatusLogModel.host
                    == latest_running_version_value_subquery.c.host,
                ),
                SCHStatusLogModel.event_time
                == latest_running_version_value_subquery.c.event_time,
            ),
        )
        .outerjoin(
            latest_configured_version_value_subquery,
            and_(
                SCHStatusLogModel.service
                == latest_configured_version_value_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLogModel.component == None,
                    SCHStatusLogModel.component
                    == latest_configured_version_value_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLogModel.host == None,
                    SCHStatusLogModel.host
                    == latest_configured_version_value_subquery.c.host,
                ),
                SCHStatusLogModel.event_time
                == latest_configured_version_value_subquery.c.event_time,
            ),
        )
        .outerjoin(
            latest_to_config_value_subquery,
            and_(
                SCHStatusLogModel.service == latest_to_config_value_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLogModel.component == None,
                    SCHStatusLogModel.component
                    == latest_to_config_value_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLogModel.host == None,
                    SCHStatusLogModel.host == latest_to_config_value_subquery.c.host,
                ),
                SCHStatusLogModel.event_time
                == latest_to_config_value_subquery.c.event_time,
            ),
        )
        .outerjoin(
            latest_to_restart_value_subquery,
            and_(
                SCHStatusLogModel.service == latest_to_restart_value_subquery.c.service,
                # Check for null component or if they are equal
                or_(
                    SCHStatusLogModel.component == None,
                    SCHStatusLogModel.component
                    == latest_to_restart_value_subquery.c.component,
                ),
                # Check for null host or if they are equal
                or_(
                    SCHStatusLogModel.host == None,
                    SCHStatusLogModel.host == latest_to_restart_value_subquery.c.host,
                ),
                SCHStatusLogModel.event_time
                == latest_to_restart_value_subquery.c.event_time,
            ),
        )
        .group_by(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
        )
        .order_by(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
        )
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
    return session.query(DeploymentModel).filter_by(status="PLANNED").one_or_none()


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

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import and_, desc, func, or_, select, tuple_
from sqlalchemy.orm import joinedload

from tdp.core.models import DeploymentLog, ServiceComponentLog


def get_latest_success_service_component_version_query():
    max_depid_label = f"max_{ServiceComponentLog.deployment_id.name}"

    latest_success_for_each_service = (
        select(
            func.max(ServiceComponentLog.deployment_id).label(max_depid_label),
            ServiceComponentLog.service,
            ServiceComponentLog.component,
        )
        .group_by(ServiceComponentLog.service, ServiceComponentLog.component)
        .subquery()
    )
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


def get_deployment(session_class, deployment_id):
    query = (
        select(DeploymentLog)
        .options(
            joinedload(DeploymentLog.service_components),
            joinedload(DeploymentLog.operations),
        )
        .where(DeploymentLog.id == deployment_id)
        .order_by(DeploymentLog.id)
    )

    with session_class() as session:
        deployment_log = session.execute(query).unique().scalar_one_or_none()
        if deployment_log is None:
            raise Exception(f"Deployment id {deployment_id} does not exist")
        return deployment_log


def get_last_deployment(session_class):
    query = (
        select(DeploymentLog)
        .options(
            joinedload(DeploymentLog.service_components),
            joinedload(DeploymentLog.operations),
        )
        .order_by(DeploymentLog.id.desc())
        .limit(1)
    )

    with session_class() as session:
        deployment_log = session.execute(query).unique().scalar_one_or_none()
        if deployment_log is None:
            raise Exception(f"No deployments")
        return deployment_log

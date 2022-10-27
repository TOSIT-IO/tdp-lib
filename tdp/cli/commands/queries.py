# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import and_, desc, func, select, tuple_
from sqlalchemy.orm import joinedload

from tdp.core.models import DeploymentLog, OperationLog, ServiceLog
from tdp.core.runner.executor import StateEnum


def get_latest_success_service_version_query():
    max_depid_label = f"max_{ServiceLog.deployment_id.name}"

    latest_success_for_each_service = (
        select(
            func.max(ServiceLog.deployment_id).label(max_depid_label),
            ServiceLog.service,
        )
        .join(
            OperationLog,
            and_(
                ServiceLog.deployment_id == OperationLog.deployment_id,
                OperationLog.operation.like(ServiceLog.service + "\\_%", escape="\\"),
            ),
        )
        .filter(OperationLog.state == StateEnum.SUCCESS.value)
        .group_by(ServiceLog.service)
    )

    return (
        select(
            ServiceLog.deployment_id,
            ServiceLog.service,
            func.substr(ServiceLog.version, 1, 7),
        )
        .filter(
            tuple_(ServiceLog.deployment_id, ServiceLog.service).in_(
                latest_success_for_each_service
            )
        )
        .order_by(desc(ServiceLog.deployment_id))
    )


def get_deployment(session_class, deployment_id):
    query = (
        select(DeploymentLog)
        .options(
            joinedload(DeploymentLog.services), joinedload(DeploymentLog.operations)
        )
        .where(DeploymentLog.id == deployment_id)
        .order_by(DeploymentLog.id)
    )

    with session_class() as session:
        deployment_log = session.execute(query).unique().scalar_one_or_none()
        if deployment_log is None:
            raise click.ClickException(f"Deployment id {deployment_id} does not exist")
        return deployment_log

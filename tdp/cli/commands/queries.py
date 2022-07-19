# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import and_, desc, func, select, tuple_

from tdp.core.models import OperationLog, ServiceLog
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

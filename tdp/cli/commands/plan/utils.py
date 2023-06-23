# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy.orm import Session

from tdp.core.models import DeploymentLog


def get_planned_deployment_log(session: Session) -> DeploymentLog:
    return session.query(DeploymentLog).filter_by(state="PLANNED").one_or_none()

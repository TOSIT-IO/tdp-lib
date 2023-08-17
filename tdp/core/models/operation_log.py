# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tdp.core.models.base import Base
from tdp.core.models.state_enum import OperationStateEnum
from tdp.core.operation import HOST_NAME_MAX_LENGTH, OPERATION_NAME_MAX_LENGTH

if TYPE_CHECKING:
    from tdp.core.models import DeploymentLog


class OperationLog(Base):
    """Operation log model.

    Hold past and current operation information linked to a deployment.
    """

    __tablename__ = "operation_log"

    deployment_id: Mapped[int] = mapped_column(
        ForeignKey("deployment_log.id"), primary_key=True, doc="Deployment log id."
    )
    operation_order: Mapped[int] = mapped_column(
        primary_key=True, doc="Operation order."
    )
    operation: Mapped[str] = mapped_column(
        String(OPERATION_NAME_MAX_LENGTH), doc="Operation name."
    )
    host: Mapped[Optional[str]] = mapped_column(
        String(HOST_NAME_MAX_LENGTH), doc="Operation host."
    )
    extra_vars: Mapped[Optional[list[str]]] = mapped_column(
        JSON(none_as_null=True), doc="Extra vars."
    )
    start_time: Mapped[Optional[datetime]] = mapped_column(doc="Operation start time.")
    end_time: Mapped[Optional[datetime]] = mapped_column(doc="Operation end time.")
    state: Mapped[OperationStateEnum] = mapped_column(doc="Operation state.")
    logs: Mapped[Optional[bytes]] = mapped_column(doc="Operation logs.")

    deployment: Mapped[DeploymentLog] = relationship(
        back_populates="operations", doc="Deployment log."
    )

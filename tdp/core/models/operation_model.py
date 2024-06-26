# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

from sqlalchemy import JSON, ForeignKey, LargeBinary, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tdp.core.constants import (
    HOST_NAME_MAX_LENGTH,
    LOGS_MAX_LENGTH,
    OPERATION_NAME_MAX_LENGTH,
)
from tdp.core.models.base_model import BaseModel
from tdp.core.models.enums import OperationStateEnum

if TYPE_CHECKING:
    from tdp.core.models import DeploymentModel


class OperationModel(BaseModel):
    """Operation model.

    Hold past and current operation information linked to a deployment.
    """

    __tablename__ = "operation"

    deployment_id: Mapped[int] = mapped_column(
        ForeignKey("deployment.id"), primary_key=True, doc="deployment id."
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
    logs: Mapped[Optional[bytes]] = mapped_column(
        LargeBinary(LOGS_MAX_LENGTH), doc="Operation logs."
    )

    deployment: Mapped[DeploymentModel] = relationship(
        back_populates="operations", doc="deployment."
    )

    def _formater(self, key: str, value: Optional[Any]) -> str:
        if key == "logs":
            return value.decode("utf-8")[:20] + "..." if value else ""
        return super()._formater(key, value)

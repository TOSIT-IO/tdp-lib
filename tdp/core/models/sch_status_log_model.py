# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from tdp.core.constants import (
    COMPONENT_NAME_MAX_LENGTH,
    HOST_NAME_MAX_LENGTH,
    MESSAGE_MAX_LENGTH,
    SERVICE_NAME_MAX_LENGTH,
    VERSION_MAX_LENGTH,
)
from tdp.core.models.base_model import BaseModel
from tdp.core.models.enums import SCHStatusLogSourceEnum


class SCHStatusLogModel(BaseModel):
    """Hold what component version are deployed."""

    __tablename__ = "sch_status_log"

    id: Mapped[int] = mapped_column(
        doc="Unique id of the cluster status log.", primary_key=True
    )
    event_time: Mapped[datetime] = mapped_column(
        default=datetime.utcnow,
        doc="Timestamp of the component version log.",
    )
    service: Mapped[str] = mapped_column(
        String(SERVICE_NAME_MAX_LENGTH), doc="Service name."
    )
    component: Mapped[Optional[str]] = mapped_column(
        String(COMPONENT_NAME_MAX_LENGTH),
        doc="Component name.",
    )
    host: Mapped[Optional[str]] = mapped_column(
        String(HOST_NAME_MAX_LENGTH), doc="Host name."
    )
    running_version: Mapped[Optional[str]] = mapped_column(
        String(VERSION_MAX_LENGTH), doc="Running version of the component."
    )
    configured_version: Mapped[Optional[str]] = mapped_column(
        String(VERSION_MAX_LENGTH), doc="Configured version of the component."
    )
    to_config: Mapped[Optional[bool]] = mapped_column(
        doc="True if the component need to be configured."
    )
    to_restart: Mapped[Optional[bool]] = mapped_column(
        doc="True if the component need to be restarted."
    )
    source: Mapped[SCHStatusLogSourceEnum] = mapped_column(
        doc="Source of the status log.",
    )
    deployment_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("deployment.id"),
        doc="Related deployment id (if applicable).",
    )
    message: Mapped[Optional[str]] = mapped_column(
        String(MESSAGE_MAX_LENGTH),
        doc="Description of the change when manually edited.",
    )

    def _formater(self, key: str, value: Any):
        """Format a value for printing."""
        if key in ["running_version", "configured_version"] and value:
            return str(value[:7])
        return super()._formater(key, value)

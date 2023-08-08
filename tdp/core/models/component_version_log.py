# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tdp.core.models.base import Base
from tdp.core.operation import COMPONENT_NAME_MAX_LENGTH, SERVICE_NAME_MAX_LENGTH
from tdp.core.repository.repository import VERSION_MAX_LENGTH

if TYPE_CHECKING:
    from tdp.core.models.deployment_log import DeploymentLog


class ComponentVersionLog(Base):
    """Hold what component version are deployed.

    Attributes:
        id: Component version log id.
        deployment_id: Deployment log id.
        service: Service name.
        component: Component name.
        version: Component version.
    """

    __tablename__ = "component_version_log"

    id: Mapped[int] = mapped_column(primary_key=True)
    deployment_id: Mapped[int] = mapped_column(ForeignKey("deployment_log.id"))
    service: Mapped[str] = mapped_column(String(SERVICE_NAME_MAX_LENGTH))
    component: Mapped[Optional[str]] = mapped_column(String(COMPONENT_NAME_MAX_LENGTH))
    version: Mapped[str] = mapped_column(String(VERSION_MAX_LENGTH))

    deployment: Mapped[DeploymentLog] = relationship(back_populates="component_version")

    __table_args__ = (UniqueConstraint("deployment_id", "service", "component"),)

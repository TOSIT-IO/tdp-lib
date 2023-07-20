# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Column, String, Boolean

from tdp.core.models.base import Base
from tdp.core.operation import SERVICE_NAME_MAX_LENGTH, COMPONENT_NAME_MAX_LENGTH


class StaleComponent(Base):
    """Hold what components are staled.

    Attributes:
        service_name (str): Service name.
        component_name (str): Component name.
        to_reconfigure (bool): Is configured flag.
        to_restart (bool): Is restarted flag.
    """

    __tablename__ = "stale_component"

    service_name = Column(
        String(length=SERVICE_NAME_MAX_LENGTH), primary_key=True, nullable=False
    )
    component_name = Column(
        String(length=COMPONENT_NAME_MAX_LENGTH), primary_key=True, nullable=True
    )
    to_reconfigure = Column(Boolean, default=False)
    to_restart = Column(Boolean, default=False)

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from typing import Any, Optional

from tdp.core.entities.hosted_entity import HostedEntity, HostedServiceComponent
from tdp.core.models.sch_status_log_model import SCHStatusLogModel


# TODO: add latest_update column
@dataclass
class HostedEntityStatus:
    entity: HostedEntity
    running_version: Optional[str] = None
    configured_version: Optional[str] = None
    to_config: Optional[bool] = None
    to_restart: Optional[bool] = None

    @property
    def is_stale(self) -> bool:
        """Return True if the entity status is stale, False otherwise."""
        return bool(self.to_config or self.to_restart)

    def update(
        self,
        running_version: Optional[str] = None,
        configured_version: Optional[str] = None,
        to_config: Optional[bool] = None,
        to_restart: Optional[bool] = None,
    ) -> Optional[SCHStatusLogModel]:
        """Update the status of a service component host.

        Args:
            running_version: Running version of the component.
            configured_version: Configured version of the component.
            to_config: True if the component need to be configured.
            to_restart: True if the component need to be restarted.

        Returns:
            SCHStatusLog instance if the status was updated, None otherwise.
        """
        # Return early if nothing changed
        if (
            running_version == self.running_version
            and configured_version == self.configured_version
            and to_config == self.to_config
            and to_restart == self.to_restart
        ):
            return

        # Base log
        log = SCHStatusLogModel(
            service=self.entity.name.service,
            component=(
                self.entity.name.component
                if isinstance(self.entity, HostedServiceComponent)
                else None
            ),
            host=self.entity.host,
        )

        if running_version is not None and running_version != self.running_version:
            self.running_version = running_version
            log.running_version = running_version

        if (
            configured_version is not None
            and configured_version != self.configured_version
        ):
            self.configured_version = configured_version
            log.configured_version = configured_version

        if to_config is not None and to_config != self.to_config:
            self.to_config = to_config
            log.to_config = to_config

        if to_restart is not None and to_restart != self.to_restart:
            self.to_restart = to_restart
            log.to_restart = to_restart

        return log

    def export_tabulate(self) -> dict[str, Any]:
        """Return the status in format that can be printed by the tabulate function."""
        return {
            "service": self.entity.name.service,
            "component": (
                self.entity.name.component
                if isinstance(self.entity, HostedServiceComponent)
                else None
            ),
            "host": self.entity.host,
            "running_version": _format_version(self.running_version),
            "configured_version": _format_version(self.configured_version),
            "to_config": _format_true_bool(self.to_config),
            "to_restart": _format_true_bool(self.to_restart),
        }


def _format_version(version: Optional[str], length: int = 7) -> Optional[str]:
    """Truncate the version to a specific length."""
    if version is None:
        return None
    return version[:length]


def _format_true_bool(value: Optional[bool]) -> str:
    """Display a boolean value as a string. Filter out False and None values."""
    return str(value) if value else ""


def _format_false_bool(value: Optional[bool]) -> str:
    """Display a boolean value as a string. Filter out True values."""
    return str(value) if value is False else ""

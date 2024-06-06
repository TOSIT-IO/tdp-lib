# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Iterable, Optional

from sqlalchemy import Engine
from sqlalchemy.orm import sessionmaker

from tdp.cli.queries import (
    create_get_sch_latest_status_statement,
)
from tdp.core.cluster_status import ClusterStatus
from tdp.core.entities.hostable_entity_name import create_hostable_entity_name
from tdp.core.entities.hosted_entity import create_hosted_entity
from tdp.core.entities.hosted_entity_status import HostedEntityStatus
from tdp.core.models.deployment_model import DeploymentModel
from tdp.core.models.operation_model import OperationModel


class Dao:
    def __init__(self, engine: Engine, commit_on_exit: bool = False):
        self.session_maker = sessionmaker(bind=engine)
        self.commit_on_exit = commit_on_exit

    def __enter__(self):
        self._session = self.session_maker()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self._session.rollback()
        elif self.commit_on_exit:
            self._session.commit()
        self._session.close()

    def _check_session(self):
        if self._session is None:
            raise Exception("Session not initialized")

    @property
    def session(self):
        self._check_session()
        return self._session

    def get_cluster_status(self) -> ClusterStatus:
        return ClusterStatus(self.get_hosted_entity_statuses(filter_active=True))

    def get_hosted_entity_statuses(
        self,
        service: Optional[str] = None,
        component: Optional[str] = None,
        hosts: Optional[Iterable[str]] = None,
        filter_stale: Optional[bool] = None,
        filter_active: Optional[bool] = None,
    ) -> list[HostedEntityStatus]:
        """Get the status of the hosted entities.

        Args:
            service: Service to filter.
            component: Component to filter.
            hosts: Hosts to filter.
            filter_stale: Whether to filter stale statuses.
        """
        self._check_session()
        stmt = create_get_sch_latest_status_statement(
            service_to_filter=service,
            component_to_filter=component,
            hosts_to_filter=hosts,
            filter_stale=filter_stale,
            filter_active=filter_active,
        )
        return [
            HostedEntityStatus(
                entity=create_hosted_entity(
                    name=create_hostable_entity_name(
                        service_name=status.service, component_name=status.component
                    ),
                    host=status.host,
                ),
                running_version=status.latest_running_version,
                configured_version=status.latest_configured_version,
                to_config=(
                    bool(status.latest_to_config)
                    if status.latest_to_config is not None
                    else None
                ),
                to_restart=(
                    bool(status.latest_to_restart)
                    if status.latest_to_restart is not None
                    else None
                ),
                is_active=(
                    bool(status.latest_is_active)
                    if status.latest_is_active is not None
                    else None
                ),
            )
            for status in self.session.execute(stmt).all()
        ]

    def get_deployment(self, id: int) -> Optional[DeploymentModel]:
        """Get a deployment by ID.

        Args:
            id: Deployment ID.
        """
        self._check_session()
        return self.session.get(DeploymentModel, id)

    def get_operation(
        self, deployment_id: int, operation_name: str
    ) -> list[OperationModel]:
        """Get an operation.

        Args:
            deployment_id: The deployment ID.
            operation_name: The operation name.
        """
        return (
            self.session.query(OperationModel)
            .filter_by(deployment_id=deployment_id, operation=operation_name)
            .all()
        )

    def get_planned_deployment(self) -> Optional[DeploymentModel]:
        self._check_session()
        return (
            self.session.query(DeploymentModel).filter_by(state="PLANNED").one_or_none()
        )

    def get_last_deployment(self) -> Optional[DeploymentModel]:
        """Get the last deployment."""
        self._check_session()
        return self.session.query(DeploymentModel).order_by(DeploymentModel.id).first()

    def get_deployments(
        self, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> Iterable[DeploymentModel]:
        self._check_session()
        return (
            self.session.query(DeploymentModel)
            .order_by(DeploymentModel.id)
            .limit(limit)
            .offset(offset)
        )

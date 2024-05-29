# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Iterable, Optional, Union

from sqlalchemy import Engine
from sqlalchemy.orm import sessionmaker

from tdp.cli.queries import (
    create_get_sch_latest_status_statement,
    get_deployment,
    get_deployments,
    get_last_deployment,
    get_operation_records,
    get_planned_deployment,
)
from tdp.core.cluster_status import ClusterStatus
from tdp.core.entities.deployment_entity import Deployment_entity
from tdp.core.entities.hostable_entity_name import create_hostable_entity_name
from tdp.core.entities.hosted_entity import create_hosted_entity
from tdp.core.entities.hosted_entity_status import HostedEntityStatus
from tdp.core.entities.operation_entity import Operation_entity


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

    def get_deployments_dao(self, limit: int, offset: int):
        self._check_session()
        deployments_records = get_deployments(limit=limit, offset=offset)
        return [
            Deployment_entity(
                id=first_element.id,
                options=first_element.options,
                start_time=first_element.start_time,
                end_time=first_element.end_time,
                state=first_element.state,
                deployment_type=first_element.deployment_type,
                operations=first_element.operations,
            )
            for first_element, *others in self.session.execute(
                deployments_records
            ).all()
        ]

    def get_deployment_dao(self, deployment_id: int) -> Deployment_entity:
        self._check_session()
        deployments_records = get_deployment(deployment_id=deployment_id)
        first_element, *others = self.session.execute(deployments_records).one()
        return Deployment_entity(
            id=first_element.id,
            options=first_element.options,
            start_time=first_element.start_time,
            end_time=first_element.end_time,
            state=first_element.state,
            deployment_type=first_element.deployment_type,
            operations=first_element.operations,
        )

    def get_last_deployment_dao(self) -> Deployment_entity:
        self._check_session()
        deployments_records = get_last_deployment()
        first_element, *others = self.session.execute(deployments_records).one()
        return Deployment_entity(
            id=first_element.id,
            options=first_element.options,
            start_time=first_element.start_time,
            end_time=first_element.end_time,
            state=first_element.state,
            deployment_type=first_element.deployment_type,
            operations=first_element.operations,
        )

    def get_planned_deployment_dao(self) -> Union[Deployment_entity, None]:
        self._check_session()
        deployment_record = get_planned_deployment()
        if deployment_tuple := self.session.execute(deployment_record).one_or_none():
            first_element, *other = deployment_tuple
            return Deployment_entity(
                id=first_element.id,
                options=first_element.options,
                start_time=first_element.start_time,
                end_time=first_element.end_time,
                state=first_element.state,
                deployment_type=first_element.deployment_type,
                operations=first_element.operations,
            )
        else:
            return

    def get_operation_dao(
        self, deployment_id, operation_name
    ) -> list[Operation_entity]:
        self._check_session()
        operation_record = get_operation_records(
            deployment_id=deployment_id, operation_name=operation_name
        )
        return [
            Operation_entity(
                deployment_id=operation.deployment_id,
                operation_order=operation.operation_order,
                operation=operation.operation,
                host=operation.host,
                extra_vars=operation.extra_vars,
                start_time=operation.start_time,
                end_time=operation.end_time,
                state=operation.state,
                logs=operation.logs,
            )
            for operation, *other in self.session.execute(operation_record).all()
        ]

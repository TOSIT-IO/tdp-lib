# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Iterable, NamedTuple, Optional

from sqlalchemy import Engine, Select, and_, case, desc, func, or_, select
from sqlalchemy.orm import sessionmaker

from tdp.core.cluster_status import ClusterStatus
from tdp.core.collections import Collections
from tdp.core.dag import Dag
from tdp.core.entities.hostable_entity_name import create_hostable_entity_name
from tdp.core.entities.hosted_entity import (
    HostedEntity,
    HostedServiceComponent,
    create_hosted_entity,
)
from tdp.core.entities.hosted_entity_status import HostedEntityStatus
from tdp.core.models.deployment_model import DeploymentModel
from tdp.core.models.enums import SCHStatusLogSourceEnum
from tdp.core.models.operation_model import OperationModel
from tdp.core.models.sch_status_log_model import SCHStatusLogModel
from tdp.core.variables.cluster_variables import ClusterVariables


def _create_last_value_statement(column, non_null=False):
    """Create a windowed query that returns last value of a column.

    Args:
        column: The column to return the last value of.
        non_null: Whether to return the last non-null value.
    """
    order_by = SCHStatusLogModel.event_time.desc()
    if non_null:
        order_by = case((column == None, 0), else_=1).desc(), order_by
    return func.first_value(column).over(
        partition_by=(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
        ),
        order_by=order_by,
    )


class SCHLatestStatus(NamedTuple):
    service: str
    component: Optional[str]
    host: Optional[str]
    latest_running_version: Optional[str]
    latest_configured_version: Optional[str]
    latest_to_config: Optional[bool]
    latest_to_restart: Optional[bool]
    latest_is_active: Optional[bool]


def _create_get_sch_latest_status_statement(
    service_to_filter: Optional[str] = None,
    component_to_filter: Optional[str] = None,
    hosts_to_filter: Optional[Iterable[str]] = None,
    filter_stale: Optional[bool] = None,
    filter_active: Optional[bool] = None,
) -> Select[SCHLatestStatus]:
    """Create a query to get the cluster status.

    Args:
        service_to_filter: The service to filter.
        component_to_filter: The component to filter.
        host_to_filter: The host to filter.
        filter_stale: Whether to filter stale status.
          True for stale, False for not stale, None for all.
    """
    subquery_filter = []
    if service_to_filter:
        subquery_filter.append(SCHStatusLogModel.service == service_to_filter)
    if component_to_filter:
        subquery_filter.append(SCHStatusLogModel.component == component_to_filter)
    if hosts_to_filter:
        subquery_filter.append(SCHStatusLogModel.host.in_(hosts_to_filter))

    subq = (
        select(
            SCHStatusLogModel.service,
            SCHStatusLogModel.component,
            SCHStatusLogModel.host,
            _create_last_value_statement(
                SCHStatusLogModel.running_version, non_null=True
            ).label("latest_running_version"),
            _create_last_value_statement(
                SCHStatusLogModel.configured_version, non_null=True
            ).label("latest_configured_version"),
            _create_last_value_statement(
                SCHStatusLogModel.to_config, non_null=True
            ).label("latest_to_config"),
            _create_last_value_statement(
                SCHStatusLogModel.to_restart, non_null=True
            ).label("latest_to_restart"),
            _create_last_value_statement(
                SCHStatusLogModel.is_active, non_null=True
            ).label("latest_is_active"),
        )
        .filter(*subquery_filter)
        .distinct()
        .subquery()
    )

    query_filter = []
    if filter_stale is True:
        query_filter.append(
            or_(
                subq.c.latest_to_config.is_(True),
                subq.c.latest_to_restart.is_(True),
            )
        )
    elif filter_stale is False:
        query_filter.append(
            and_(
                subq.c.latest_to_config.is_not(True),
                subq.c.latest_to_restart.is_not(True),
            )
        )

    if filter_active is True:
        query_filter.append(subq.c.latest_is_active.is_not(False))
    elif filter_active is False:
        query_filter.append(subq.c.latest_is_active.is_(False))

    return select(subq).filter(*query_filter)


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
        stmt = _create_get_sch_latest_status_statement(
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

    def get_hosted_entity_statuses_history(
        self,
        limit,
        service: Optional[str] = None,
        component: Optional[str] = None,
        hosts: Optional[Iterable[str]] = None,
        filter_stale: Optional[bool] = None,
        filter_active: Optional[bool] = None,
    ) -> list[SCHStatusLogModel]:
        """Get the status of the hosted entities.

        Args:
            limit: limits the rows to display
            service: Service to filter.
            component: Component to filter.
            hosts: Hosts to filter.
            filter_stale: Whether to filter stale statuses.
            filter_active: Whether to filter active statuses.
        """
        self._check_session()
        query_filter = []
        if service:
            query_filter.append(SCHStatusLogModel.service == service)
        if component:
            query_filter.append(SCHStatusLogModel.component == component)
        if hosts:
            query_filter.append(SCHStatusLogModel.host.in_(hosts))

        if filter_stale is True:
            query_filter.append(
                or_(
                    SCHStatusLogModel.to_config.is_(True),
                    SCHStatusLogModel.to_restart.is_(True),
                )
            )
        elif filter_stale is False:
            query_filter.append(
                and_(
                    SCHStatusLogModel.to_config.is_not(True),
                    SCHStatusLogModel.to_restart.is_not(True),
                )
            )

        if filter_active is True:
            query_filter.append(SCHStatusLogModel.is_active.is_not(False))
        elif filter_active is False:
            query_filter.append(SCHStatusLogModel.is_active.is_(False))
        return (
            self.session.query(SCHStatusLogModel)
            .filter(*query_filter)
            .limit(limit)
            .all()
        )

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
        return (
            self.session.query(DeploymentModel)
            .order_by(desc(DeploymentModel.id))
            .first()
        )

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

    def generate_stale_sch_logs(
        self,
        *,
        cluster_variables: ClusterVariables,
        collections: Collections,
    ) -> set[SCHStatusLogModel]:
        """Generate logs for components that need to be configured or restarted.

        This method identifies components that have undergone changes in their
        versions and determines if they need to be configured, restarted, or both.

        Note: If a component has neither config or restart operations, it is not
        considered stale and is excluded from the results.

        Args:
            cluster_variables: Current configuration.
            collections: Collections instance.

        Returns:
            Set of SCHStatusLog.
        """
        hosted_entity_status: list[HostedEntityStatus] = (
            self.get_hosted_entity_statuses()
        )

        logs: dict[HostedEntity, SCHStatusLogModel] = {}
        source_reconfigure_operations: set[str] = set()

        modified_entities = cluster_variables.get_modified_entities(
            ClusterStatus(hosted_entity_status).values()
        )

        # Return early if no entity has modified configurations
        if len(modified_entities) == 0:
            return set()

        # Get the list of services of the current hosted_entities in the database
        hosted_entities: list[HostedEntity] = [
            entity_status.entity for entity_status in hosted_entity_status
        ]
        hosted_entity_services: list[str] = [
            entity.name.service for entity in hosted_entities
        ]

        # Create logs for the modified entities
        for entity in modified_entities:
            config_operation = collections.operations.get(f"{entity.name}_config")
            start_operation = collections.operations.get(f"{entity.name}_start")
            restart_operation = collections.operations.get(f"{entity.name}_restart")

            # Add the config and start operations to the set to get their descendants
            if config_operation:
                source_reconfigure_operations.add(config_operation.name)
            if start_operation:
                source_reconfigure_operations.add(start_operation.name)

            # Create a log to update the stale status of the entity if a config and/or
            # restart operations are available
            # Only source hosts affected by the modified configuration are considered as
            # stale (while all hosts are considered as stale for the descendants)
            if (
                config_operation
                and entity in hosted_entities
                or restart_operation
                and entity in hosted_entities
            ):
                log = logs.setdefault(
                    entity,
                    SCHStatusLogModel(
                        service=entity.name.service,
                        component=(
                            entity.name.component
                            if isinstance(entity, HostedServiceComponent)
                            else None
                        ),
                        host=entity.host,
                        source=SCHStatusLogSourceEnum.STALE,
                    ),
                )
                if config_operation:
                    log.to_config = True
                if restart_operation:
                    log.to_restart = True

        # Create logs for the descendants of the modified entities
        for operation in Dag(collections).get_operation_descendants(
            nodes=list(source_reconfigure_operations), restart=True
        ):
            # Only create a log when config or restart operation is available
            if (
                operation.action_name not in ["config", "restart"]
                or operation.service_name not in hosted_entity_services
            ):
                continue

            # Create a log for each host where the entity is deployed
            for host in operation.host_names:
                log = logs.setdefault(
                    create_hosted_entity(
                        create_hostable_entity_name(
                            operation.service_name, operation.component_name
                        ),
                        host,
                    ),
                    SCHStatusLogModel(
                        service=operation.service_name,
                        component=operation.component_name,
                        host=host,
                        source=SCHStatusLogSourceEnum.STALE,
                    ),
                )
                if operation.action_name == "config":
                    log.to_config = True
                elif operation.action_name == "restart":
                    log.to_restart = True

        return set(logs.values())

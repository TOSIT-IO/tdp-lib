# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Iterable, NamedTuple, Optional

from sqlalchemy import Engine, Select, and_, case, desc, func, or_, select
from sqlalchemy.orm import aliased, sessionmaker

from tdp.core.cluster_status import ClusterStatus
from tdp.core.entities.entity_name import create_entity_name
from tdp.core.entities.hosted_entity import create_hosted_entity
from tdp.core.entities.hosted_entity_status import HostedEntityStatus
from tdp.core.models.deployment_model import DeploymentModel
from tdp.core.models.operation_model import OperationModel
from tdp.core.models.sch_status_log_model import SCHStatusLogModel


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


def _create_get_sch_latest_status_statement(
    service_to_filter: Optional[str] = None,
    component_to_filter: Optional[str] = None,
    hosts_to_filter: Optional[Iterable[str]] = None,
    filter_stale: Optional[bool] = None,
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
        return ClusterStatus(self.get_hosted_entity_statuses())

    def get_hosted_entity_statuses(
        self,
        service: Optional[str] = None,
        component: Optional[str] = None,
        hosts: Optional[Iterable[str]] = None,
        filter_stale: Optional[bool] = None,
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
        )
        return [
            HostedEntityStatus(
                entity=create_hosted_entity(
                    name=create_entity_name(
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
    ) -> list[SCHStatusLogModel]:
        """Get the status of the hosted entities.

        Args:
            limit: limits the rows to display
            service: Service to filter.
            component: Component to filter.
            hosts: Hosts to filter.
            filter_stale: Whether to filter stale statuses.
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

    def get_operations_by_name(
        self, deployment_id: int, operation_name: str
    ) -> list[OperationModel]:
        """Get all operations for a deployment from their name.

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

    def get_last_deployments(
        self, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> Iterable[DeploymentModel]:
        """Get last deployments in ascending order.

        Use limit and offset to paginate the results.

        Args:
            limit: The maximum number of deployments to return.
            offset: The number of deployments to skip.

        Example:
            # Get the last 10 deployments.
            >>> dao.get_last_deployments(limit=10, offset=0)
            # Get the next 10 deployments.
            >>> dao.get_last_deployments(limit=10, offset=10)
        """
        self._check_session()

        # Get the last deployments (in descending order).
        reversed_deployments_query = self.session.query(DeploymentModel.id).order_by(
            desc(DeploymentModel.id)
        )
        # Apply limit and offset.
        if limit is not None:
            reversed_deployments_query = reversed_deployments_query.limit(limit)
        if offset is not None:
            reversed_deployments_query = reversed_deployments_query.offset(offset)
        # Alias the subquery for convenience.
        reversed_deployments = aliased(
            DeploymentModel, reversed_deployments_query.subquery()
        )
        # Return the deployments in ascending order.
        return (
            self.session.query(reversed_deployments)
            .order_by(reversed_deployments.id)
            .all()
        )

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Literal, NamedTuple, Optional

from exceptiongroup import ExceptionGroup
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from tabulate import tabulate

from tdp.core.constants import OPERATION_SLEEP_NAME, OPERATION_SLEEP_VARIABLE
from tdp.core.dag import Dag
from tdp.core.entities.operation import (
    NotPlaybookOperationError,
    OperationCannotBeLimitedError,
    OperationName,
    OperationNotAvailableOnHostError,
    PlaybookOperation,
)
from tdp.core.filters import FilterFactory
from tdp.core.models.base_model import BaseModel
from tdp.core.models.enums import (
    DeploymentStateEnum,
    DeploymentTypeEnum,
    FilterTypeEnum,
    OperationStateEnum,
)
from tdp.core.models.operation_model import OperationModel

if TYPE_CHECKING:
    from tdp.core.collections import Collections
    from tdp.core.entities.hosted_entity_status import HostedEntityStatus
    from tdp.core.entities.operation import Operation

logger = logging.getLogger(__name__)


class NoOperationMatchError(Exception):
    pass


class NothingToReconfigureError(Exception):
    pass


class NothingToResumeError(Exception):
    pass

class NothingToDeployError(Exception):
    pass

class MissingOperationError(Exception):
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        super().__init__(f"Operation {operation_name} not found.")


class MissingHostForOperationError(Exception):
    def __init__(self, operation: Operation, host_name: str):
        self.operation = operation
        self.host_name = host_name
        available_hosts = []
        if isinstance(operation, PlaybookOperation):
            available_hosts = operation.playbook.hosts
        super().__init__(
            f"Host {host_name} not found for operation {operation.name}."
            f"Available hosts are {available_hosts}."
        )


class _OperationHost(NamedTuple):
    operation: Operation
    host: Optional[str]


class DeploymentModel(BaseModel):
    """Deployment model.

    Hold past and current deployment information.
    """

    __tablename__ = "deployment"

    id: Mapped[int] = mapped_column(primary_key=True, doc="deployment id.")
    options: Mapped[Optional[dict]] = mapped_column(
        JSON(none_as_null=True), doc="Deployment options."
    )
    start_time: Mapped[Optional[datetime]] = mapped_column(doc="Deployment start time.")
    end_time: Mapped[Optional[datetime]] = mapped_column(doc="Deployment end time.")
    state: Mapped[DeploymentStateEnum] = mapped_column(doc="Deployment state.")
    deployment_type: Mapped[Optional[DeploymentTypeEnum]] = mapped_column(
        doc="Deployment type."
    )

    operations: Mapped[list[OperationModel]] = relationship(
        back_populates="deployment",
        order_by="OperationModel.operation_order",
        cascade="all, delete-orphan",
        doc="List of operations.",
    )

    def __str__(self):
        return tabulate(
            [
                ["id", self.id],
                ["start_time", self.start_time],
                ["end_time", self.end_time],
                ["state", self.state],
            ],
            tablefmt="plain",
        )

    @staticmethod
    def from_dag(
        dag: Dag,
        targets: Optional[Iterable[str]] = None,
        sources: Optional[Iterable[str]] = None,
        filter_expression: Optional[str] = None,
        filter_type: Optional[FilterTypeEnum] = None,
        restart: bool = False,
        reverse: bool = False,
        stop: bool = False,
        rolling_interval: Optional[int] = None,
        host_names: Optional[Iterable[str]] = None,
    ) -> DeploymentModel:
        """Generate a deployment plan from a DAG.

        Log if an operation can't be limited on the provided host (if specified).

        Raises:
            NoOperationMatchError: if no operation match the provided parameters.
        """
        operations = dag.get_operations(
            sources=sources,
            targets=targets,
            restart=restart,
            stop=stop,
        )

        if filter_expression:
            filter_type = filter_type or FilterTypeEnum.GLOB
            filter = FilterFactory.create_filter(
                # Default to glob if no filter type is provided
                filter_type,
                filter_expression,
            )
            operations = filter(operations)

        if len(operations) == 0:
            raise NoOperationMatchError(
                "Combination of parameters resulted into an empty list of Operations."
            )

        if reverse:
            operations = reversed(operations)

        if host_names is None or len(list(host_names)) == 0:
            operation_hosts = [
                _OperationHost(operation, None) for operation in operations
            ]
        else:
            # Use a dict with None values to mimic an ordered set
            # (dict preserves insertion order since Python 3.7)
            operation_hosts_tmp: dict[_OperationHost, None] = {}
            for operation in operations:
                for host in host_names:
                    try:
                        operation.check_limit(host)
                    except OperationNotAvailableOnHostError:
                        # Skip host if operation is not available on it
                        continue
                    except OperationCannotBeLimitedError as e:
                        # Display an info if operation cannot be limited to host
                        logger.info(str(e))
                        host = None
                    except NotPlaybookOperationError:
                        host = None
                    operation_hosts_tmp[_OperationHost(operation, host)] = None
            operation_hosts = list(operation_hosts_tmp.keys())
            if len(operation_hosts) == 0:
                raise NoOperationMatchError(
                    "Combination of parameters resulted into an empty list of Operations."
                )

        deployment = DeploymentModel(
            deployment_type=DeploymentTypeEnum.DAG,
            options={
                **_filter_falsy_options(
                    {
                        "sources": sources,
                        "targets": targets,
                        "filter_type": filter_type,
                        "filter_expression": filter_expression,
                        "restart": restart,
                        "stop": stop,
                        "reverse": reverse,
                    }
                ),
            },
            state=DeploymentStateEnum.PLANNED,
        )
        operation_order = 1
        for operation, host in operation_hosts:
            can_perform_rolling_restart = (
                rolling_interval is not None
                and isinstance(operation, PlaybookOperation)
                and operation.name.action == "restart"
                and len(operation.playbook.hosts) > 0
            )
            deployment.operations.append(
                OperationModel(
                    operation=operation.name.name,
                    operation_order=operation_order,
                    host=host,
                    extra_vars=None,
                    state=OperationStateEnum.PLANNED,
                )
            )
            operation_order += 1
            if can_perform_rolling_restart:
                deployment.operations.append(
                    OperationModel(
                        operation=OPERATION_SLEEP_NAME,
                        operation_order=operation_order,
                        host=None,
                        extra_vars=[f"{OPERATION_SLEEP_VARIABLE}={rolling_interval}"],
                        state=OperationStateEnum.PLANNED,
                    )
                )
                operation_order += 1
        return deployment

    @staticmethod
    def from_operations(
        collections: Collections,
        operation_names: list[str],
        host_names: Optional[Iterable[str]] = None,
        extra_vars: Optional[Iterable[str]] = None,
        rolling_interval: Optional[int] = None,
    ) -> DeploymentModel:
        """Generate a deployment plan from a list of operations.

        Raises:
            ExceptionGroup: With the list of operations that are missing from the collections or invalid.
        """
        deployment = DeploymentModel(
            deployment_type=DeploymentTypeEnum.OPERATIONS,
            options={
                "operations": operation_names,
                **_filter_falsy_options(
                    {
                        "hosts": host_names,
                        "extra_vars": extra_vars,
                        "rolling_interval": rolling_interval,
                    }
                ),
            },
            state=DeploymentStateEnum.PLANNED,
        )

        exceptions = []
        operation_order = 1
        for operation_name in operation_names:
            # Check if operation is valid.
            try:
                operation: Operation = collections.operations[operation_name]
                operation.check_limit(host_names)
            except Exception as e:
                exceptions.append(e)
                continue

            # Skip filling up operation array if at list 1 operation is invalid.
            if len(exceptions) > 0:
                continue

            can_perform_rolling_restart = (
                rolling_interval is not None
                and isinstance(operation, PlaybookOperation)
                and operation.name.action == "restart"
                and len(operation.playbook.hosts) > 0
            )
            for host_name in host_names or (
                # if restart operation with rolling and no host is specified,
                # run on all hosts
                operation.playbook.hosts  # type: ignore : operation is a PlaybookOperation
                if can_perform_rolling_restart
                else [None]
            ):
                deployment.operations.append(
                    OperationModel(
                        operation=operation.name.name,
                        operation_order=operation_order,
                        host=host_name,
                        extra_vars=list(extra_vars) if extra_vars else None,
                        state=OperationStateEnum.PLANNED,
                    )
                )
                if can_perform_rolling_restart:
                    operation_order += 1
                    deployment.operations.append(
                        OperationModel(
                            operation=OPERATION_SLEEP_NAME,
                            operation_order=operation_order,
                            host=None,
                            extra_vars=[
                                f"{OPERATION_SLEEP_VARIABLE}={rolling_interval}"
                            ],
                            state=OperationStateEnum.PLANNED,
                        )
                    )
                operation_order += 1

        # Raise all exceptions at once if any.
        if len(exceptions):
            raise ExceptionGroup("At least one operation is invalid.", exceptions)

        return deployment

    @staticmethod
    def from_operations_hosts_vars(
        collections: Collections,
        operation_host_vars_names: list[tuple[str, Optional[str], Optional[list[str]]]],
    ) -> DeploymentModel:
        """Generate a deployment plan from a list of operations, hosts and extra vars.

        Raises:
            ExceptionGroup: With the list of operations that are missing from the collections or invalid.
        """
        deployment = DeploymentModel(
            deployment_type=DeploymentTypeEnum.CUSTOM,
            state=DeploymentStateEnum.PLANNED,
        )

        exceptions = []
        for operation_order, operation_host_vars in enumerate(
            operation_host_vars_names, start=1
        ):
            operation_name, host_name, var_names = operation_host_vars
            # Check if operation is valid
            try:
                operation: Operation = collections.operations[operation_name]
                operation.check_limit(host_name)
            except Exception as e:
                exceptions.append(e)
                continue

            deployment.operations.append(
                OperationModel(
                    operation=operation_name,
                    operation_order=operation_order,
                    host=host_name,
                    extra_vars=var_names,
                    state=OperationStateEnum.PLANNED,
                )
            )

        # Raise all exceptions at once if any.
        if len(exceptions) > 0:
            raise ExceptionGroup("At least one operation is invalid", exceptions)

        return deployment

    @staticmethod
    def from_stale_hosted_entities(
        collections: Collections,
        stale_hosted_entity_statuses: list[HostedEntityStatus],
        rolling_interval: Optional[int] = None,
    ) -> DeploymentModel:
        """Generate a deployment plan for stale components.

        Log a warning if an operation is missing from the collections.

        Raises:
            NothingToReconfigureError: If no component needs to be reconfigured.
        """

        def _get_operation_host(
            status: HostedEntityStatus, action: Literal["config", "restart"]
        ) -> Optional[_OperationHost]:
            operation_name = OperationName(status.entity.name, action)
            host = status.entity.host
            try:
                operation: Operation = collections.operations[operation_name]
                operation.check_limit(status.entity.host)
            except KeyError as e:
                logger.warning(str(e) + "Skipping.")
                return
            except (
                NotPlaybookOperationError,
                OperationNotAvailableOnHostError,
                OperationCannotBeLimitedError,
            ):
                host = None
            return _OperationHost(operation, host)

        # Using a set is important here as some operation-host will be identical
        # (at least the ones were the host was set to None by _get_operation_host)
        operation_hosts: set[_OperationHost] = set()
        for status in stale_hosted_entity_statuses:
            if status.to_config and (
                config_operation := _get_operation_host(status, "config")
            ):
                operation_hosts.add(config_operation)
            if status.to_restart and (
                restart_operation := _get_operation_host(status, "restart")
            ):
                operation_hosts.add(restart_operation)
        if len(operation_hosts) == 0:
            raise NothingToReconfigureError("No component needs to be reconfigured.")

        # Sort by hosts to improve readability
        sorted_operation_hosts = sorted(
            operation_hosts,
            key=lambda x: f"{x.operation.name}_{x.host}",
        )

        # Sort operations using DAG topological sort
        dag = Dag(collections)
        reconfigure_operations_sorted = dag.topological_sort_key(
            sorted_operation_hosts,
            # topological sort only applies to start operations
            key=lambda x: str(x.operation.name).replace("_restart", "_start"),
        )

        # Generate deployment
        deployment = DeploymentModel(
            deployment_type=DeploymentTypeEnum.RECONFIGURE,
            options={
                **_filter_falsy_options(
                    {
                        "rolling_interval": rolling_interval,
                    }
                ),
            },
            state=DeploymentStateEnum.PLANNED,
        )
        operation_order = 1
        for operation, host in reconfigure_operations_sorted:
            deployment.operations.append(
                OperationModel(
                    operation=operation.name.name,
                    operation_order=operation_order,
                    host=host,
                    extra_vars=None,
                    state=OperationStateEnum.PLANNED,
                )
            )
            # Add sleep operation after each "restart"
            if rolling_interval is not None and operation.name.action == "restart":
                operation_order += 1
                deployment.operations.append(
                    OperationModel(
                        operation=OPERATION_SLEEP_NAME,
                        operation_order=operation_order,
                        host=None,
                        extra_vars=[f"{OPERATION_SLEEP_VARIABLE}={rolling_interval}"],
                        state=OperationStateEnum.PLANNED,
                    )
                )

            operation_order += 1
        return deployment

    @staticmethod
    def from_failed_deployment(
        collections: Collections, failed_deployment: "DeploymentModel"
    ) -> DeploymentModel:
        """Generate a deployment plan from a failed deployment.

        Log a warning if an operation is missing from the collections or is invalid.

        Raises:
            NothingToResumeError: If the deployment was successful.
        """
        if failed_deployment.state != DeploymentStateEnum.FAILURE:
            raise NothingToResumeError(
                f"Nothing to resume, deployment #{failed_deployment.id} "
                + f"was {failed_deployment.state}."
            )

        if len(failed_deployment.operations) == 0:
            raise NothingToResumeError(
                f"Nothing to resume, deployment #{failed_deployment.id} has no operations."
            )

        failed_operation_index = next(
            (
                index
                for index, op in enumerate(failed_deployment.operations)
                if op.state == OperationStateEnum.FAILURE
            ),
            None,
        )
        deployment = DeploymentModel(
            deployment_type=DeploymentTypeEnum.RESUME,
            options={
                "from": failed_deployment.id,
            },
            state=DeploymentStateEnum.PLANNED,
        )
        for operation_order, failed_operation in enumerate(
            failed_deployment.operations[failed_operation_index:], 1
        ):
            try:
                operation: Operation = collections.operations[
                    failed_operation.operation
                ]
                operation.check_limit(failed_operation.host)
            except KeyError:
                logger.warning(
                    f"Operation {failed_operation.operation} not found in collections. "
                    "You'll need to fix the deployment plan manually."
                )
            except (
                NotPlaybookOperationError,
                OperationNotAvailableOnHostError,
                OperationCannotBeLimitedError,
            ) as e:
                logger.warning(
                    str(e) + "You'll need to fix the deployment plan manually."
                )
            deployment.operations.append(
                OperationModel(
                    operation=failed_operation.operation,
                    operation_order=operation_order,
                    host=failed_operation.host,
                    extra_vars=failed_operation.extra_vars,
                    state=OperationStateEnum.PLANNED,
                )
            )
        return deployment

    def start_running(self) -> None:
        if self.state != DeploymentStateEnum.PLANNED:
            raise NothingToDeployError()
        self.state = DeploymentStateEnum.RUNNING
        for operation in self.operations:
            operation.state = OperationStateEnum.PENDING
        self.start_time = datetime.utcnow()


def _filter_falsy_options(options: dict) -> dict:
    """Get options without falsy values.

    Args:
        options: Options to filter.

    Returns:
        Filtered options.
    """
    return {k: v for k, v in options.items() if v}

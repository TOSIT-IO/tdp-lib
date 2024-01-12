# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

"""
This module contains fixtures and hooks for pytest.

It is used to generate tests for each rule in the rules directory of each collection.
"""

import logging
import pathlib
from typing import TYPE_CHECKING, Any, cast

import pytest
from sqlalchemy import create_engine

from tdp.cli.queries import get_sch_status
from tdp.core.cluster_status import ClusterStatus
from tdp.core.collections import Collections
from tdp.core.constants import YML_EXTENSION
from tdp.core.dag import Dag
from tdp.core.deployment import DeploymentRunner
from tdp.core.deployment.test_deployment_runner import MockExecutor
from tdp.core.models import DeploymentModel, init_database
from tdp.core.operation import Operation
from tdp.core.variables import ClusterVariables

from .constants import RULES_KEYS
from .helpers import append_collection_list_action, get_session

if TYPE_CHECKING:
    from .helpers import CollectionToTest

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)


def pytest_addoption(parser: pytest.Parser):
    """Pytest hook to add options to the pytest command"""
    parser.addoption(
        "--collection-path",
        dest="collection_paths",
        required=True,
        type=pathlib.Path,
        action=append_collection_list_action,
        help="Path to the collection to test, can be used multiple times",
    )


def pytest_generate_tests(metafunc: pytest.Metafunc):
    """Pytest hook to generate tests

    Generated tests are derived from base tests in which different fixtures are injected
    """
    # filter fixture names used by the current test, ensure "source" is the first
    argnames = sorted(
        RULES_KEYS.intersection(metafunc.fixturenames),
        key=lambda param: 0 if param == "source" else 1,
    )
    ids: list[str] = []
    argvalues: list[Any] = []
    # iterate over each rule to populate the argvalues, a test will be generated for
    # each argvalues, hence each rule
    collections_to_test = cast(
        list["CollectionToTest"], metafunc.config.getoption("collection_paths")
    )
    for collection_to_test in collections_to_test:
        for rule in collection_to_test.get_rules():
            # we will generate a set of arguments values based on the current rule
            current_argvalues = []
            # get the value of each fixture from the rule
            for fixture_name in argnames:
                current_argvalues.append(rule.get(fixture_name))
            # check if enough arguments are not null to generate a test
            if len(argnames) > 1 and argnames[0] == "source":
                # if a test has the fixture `source`, we will generate it only if any of
                # the other arguments is non null
                argvalues_to_check = current_argvalues[1:]
            else:
                # if the fixture `source` is missing, the nullity check must be
                # performed on all the arguments
                argvalues_to_check = current_argvalues
            if any(argvalues_to_check):
                # if any argument is not null for this iteration, we add this set to the
                # argvalues
                ids.append(rule.id)
                argvalues.append(current_argvalues)

    metafunc.parametrize(argnames, argvalues, ids=ids, scope="session")


@pytest.fixture
def vars(
    tmp_path_factory: pytest.TempPathFactory, collections: Collections
) -> pathlib.Path:
    """Path to the tdp_vars directory

    Directory is initialized with the defaults from the collections
    """
    tdp_vars = tmp_path_factory.mktemp("tdp_vars")
    ClusterVariables.initialize_cluster_variables(
        collections,
        tdp_vars,
    )
    return tdp_vars


@pytest.fixture(scope="session")
def collections(request: pytest.FixtureRequest) -> Collections:
    """Collections to test"""
    collections = cast(
        list["CollectionToTest"], request.config.getoption("collection_paths")
    )
    return Collections.from_collection_list(collections)


@pytest.fixture(scope="session")
def dag(collections: Collections) -> Dag:
    """DAG from the collections"""
    return Dag(collections)


@pytest.fixture
def cluster_variables(collections, vars) -> ClusterVariables:
    """Cluster variables"""
    return ClusterVariables.get_cluster_variables(collections, vars)


@pytest.fixture
def database_dsn(tmp_path_factory: pytest.TempPathFactory) -> str:
    """DSN of the database, initialized in memory"""
    # we can't use the in memory database because it needs to be shared for the whole
    # session
    db_file = tmp_path_factory.mktemp("db") / "sqlite.db"
    database_dsn = f"sqlite+pysqlite:///{db_file}"
    engine = create_engine(database_dsn, echo=True, future=True)
    init_database(engine)
    return database_dsn


@pytest.fixture
def populated_database_dsn(
    database_dsn: str,
    dag: Dag,
    cluster_variables: ClusterVariables,
    collections: Collections,
) -> str:
    """Populated database with a full DAG deployment"""
    with get_session(database_dsn) as session:
        # plan a deployment for the whole DAG
        planned_deployment = DeploymentModel.from_dag(dag)
        session.add(planned_deployment)
        session.commit()
        # run the deployment
        deployment_iterator = DeploymentRunner(
            collections=collections,
            executor=MockExecutor(),
            cluster_variables=cluster_variables,
            cluster_status=ClusterStatus.from_sch_status_rows(get_sch_status(session)),
        ).run(planned_deployment)
        for cluster_status_logs in deployment_iterator:
            if cluster_status_logs and any(cluster_status_logs):
                session.add_all(cluster_status_logs)
                for cluster_status_log in cluster_status_logs:
                    logger.info(f"Adding {cluster_status_log}")
            session.commit()
    return database_dsn


@pytest.fixture
def plan_reconfigure(
    source: str,
    populated_database_dsn: str,
    cluster_variables: ClusterVariables,
    collections: Collections,
):
    """Deployment plan for a reconfiguration of the source

    Plan is not persisted in the database.
    """
    source_filename = source + YML_EXTENSION
    service_name = source.split("_")[0]
    # raise an error if the variables are missing the source service
    if service_name not in cluster_variables.keys():
        pytest.fail(f"{service_name} variables are missing")
    # edit the source file to trigger a reconfiguration
    with cluster_variables[service_name].open_files(
        [source_filename],
        validation_message="trigger reconfigure",
        create_if_missing=True,
    ) as var_files:
        var_files[source_filename]["foo"] = "bar"
    # update the cluster status in the database
    with get_session(populated_database_dsn) as session:
        stale_status_logs = ClusterStatus.from_sch_status_rows(
            get_sch_status(session)
        ).generate_stale_sch_logs(
            cluster_variables=cluster_variables, collections=collections
        )
        session.add_all(stale_status_logs)
        session.commit()
        cluster_status = ClusterStatus.from_sch_status_rows(get_sch_status(session))
    # return the deployment plan (it is neither persisted in the database nor executed)
    return DeploymentModel.from_stale_components(collections, cluster_status)


@pytest.fixture
def stale_sc(plan_reconfigure: DeploymentModel) -> set[str]:
    """Set of stale service_components"""
    sc: set[str] = set()
    for operation in plan_reconfigure.operations:
        # TODO: would be nice to use a dedicated class to parse the operation name
        operation = Operation(operation.operation)
        if operation.component_name is None:
            sc.add(operation.service_name)
        else:
            sc.add(operation.service_name + "_" + operation.component_name)
    return sc

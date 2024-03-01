# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Iterable, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tdp.cli.queries import create_get_sch_latest_status_statement
from tdp.core.cluster_status import ClusterStatus


class Dao:
    def __init__(self, database_dsn: str, commit_on_exit: bool = False):
        engine = create_engine(database_dsn, echo=False, future=True)
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

    def get_sch_status(
        self,
        service: Optional[str] = None,
        component: Optional[str] = None,
        hosts: Optional[Iterable[str]] = None,
        filter_stale: Optional[bool] = None,
        filter_active: Optional[bool] = None,
    ) -> ClusterStatus:
        """Get the status of the cluster.

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
        res = self.session.execute(stmt).all()
        return ClusterStatus.from_sch_status_rows(res)

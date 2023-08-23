# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Optional

from sqlalchemy.orm import Session

from tdp.cli.queries import get_latest_success_component_version_log
from tdp.core.models import ComponentVersionLog


class TestGetLatestSuccessComponentVersionLog:
    def add_log(
        self,
        session: Session,
        deployment_id: int,
        service: str,
        component: Optional[str],
        host: Optional[str],
        version: str,
    ):
        """Helper function to add a log entry."""
        log = ComponentVersionLog(
            deployment_id=deployment_id,
            service=service,
            component=component,
            host=host,
            version=version,
        )
        session.add(log)
        session.commit()

    def test_latest_version_single_service_component(self, db_session: Session):
        self.add_log(db_session, 1, "service1", "component1", "host1", "v1.0")
        self.add_log(db_session, 2, "service1", "component1", "host1", "v1.1")

        logs = get_latest_success_component_version_log(db_session)
        assert len(logs) == 1
        assert logs[0].version == "v1.1"

    def test_latest_version_multiple_service_component(self, db_session: Session):
        self.add_log(db_session, 1, "service1", "component1", "host1", "v1.0")
        self.add_log(db_session, 2, "service1", "component1", "host1", "v1.1")
        self.add_log(db_session, 3, "service2", "component2", "host2", "v2.0")
        self.add_log(db_session, 4, "service2", "component2", "host2", "v2.1")

        logs = get_latest_success_component_version_log(db_session)
        assert len(logs) == 2

        # Assuming logs are ordered by service, component, and then deployment_id
        assert logs[0].version == "v1.1"
        assert logs[1].version == "v2.1"

    def test_latest_version_component_none(self, db_session: Session):
        self.add_log(db_session, 1, "service1", None, "host1", "v1.0")
        self.add_log(db_session, 2, "service1", None, "host1", "v1.1")

        logs = get_latest_success_component_version_log(db_session)
        assert len(logs) == 1
        assert logs[0].version == "v1.1"

    def test_latest_version_multiple_deployment_ids(self, db_session: Session):
        self.add_log(db_session, 1, "service1", "component1", "host1", "v1.0")
        self.add_log(db_session, 3, "service1", "component1", "host1", "v1.1")
        self.add_log(db_session, 2, "service1", "component1", "host1", "v1.2")

        logs = get_latest_success_component_version_log(db_session)
        assert len(logs) == 1

        # Latest deployment_id is 3
        assert logs[0].version == "v1.1"

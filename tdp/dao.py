# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Sequence

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tdp.cli.queries import create_get_sch_latest_status_statement
from tdp.core.models import SCHStatusRow


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

    def get_sch_status(self) -> Sequence[SCHStatusRow]:
        self._check_session()
        stmt = create_get_sch_latest_status_statement()
        return self.session.execute(stmt).all()

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tdp.core.models import init_database


def path_or_inmemory(path):
    return path.absolute() if path else ":memory:"


def get_session_class(sqlite_path=None):
    if sqlite_path and not sqlite_path.exists():
        raise ValueError(
            "a sqlite path has been set, but the path does not exist, run `tdp init`"
        )
    path = path_or_inmemory(sqlite_path)
    engine = create_engine(f"sqlite+pysqlite:///{path}", echo=False, future=True)
    session_class = sessionmaker(bind=engine)

    return session_class


def init_db(sqlite_path=None):
    path = path_or_inmemory(sqlite_path)
    engine = create_engine(
        f"sqlite+pysqlite:///{path}", echo=True, future=True
    )  # Echo = True to get logs
    init_database(engine)

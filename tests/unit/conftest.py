# Copyright 2025 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Generator

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from tdp.core.db import get_engine, get_session
from tdp.core.models import init_database
from tdp.core.models.base_model import BaseModel


@pytest.fixture()
def db_engine(db_dsn: str) -> Generator[Engine, None, None]:
    """Fixture to create a database engine.

    This fixture initializes the database schema and returns an engine that can be used
    in tests. It also ensures that the database is cleared after the test completes.
    """
    engine = get_engine(db_dsn)
    init_database(engine)
    try:
        yield engine
    finally:
        BaseModel.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture()
def db_session(db_engine: Engine) -> Generator[Session, None, None]:
    """Fixture to create a database session.

    This fixture initializes returns a session that can be used in tests. It also
    ensures that the session is closed after the test completes.
    """
    session = get_session(db_engine)
    try:
        yield session
    finally:
        session.close()

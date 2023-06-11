# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tdp.core.models import init_database


def get_session_class(database_dsn: str) -> sessionmaker:
    """Get a Session factory.

    The Session are created with echo=False to avoid logging and future=True to use
    SQLAlchemy 2.0.

    Args:
        database_dsn: DSN of the database.

    Returns:
        A Session factory.
    """
    engine = create_engine(database_dsn, echo=False, future=True)
    session_class = sessionmaker(bind=engine)

    return session_class


def init_db(database_dsn: str) -> None:
    """Initialize the database.

    Args:
        database_dsn: DSN of the database.
    """
    engine = create_engine(
        database_dsn, echo=True, future=True
    )  # Echo = True to get logs
    init_database(engine)

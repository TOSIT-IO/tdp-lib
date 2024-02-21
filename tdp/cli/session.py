# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from tdp.core.models.base_model import init_database


@contextmanager
def get_session(
    database_dsn: str, commit_on_exit: bool = False
) -> Generator[Session, None, None]:
    """Get a SQLAlchemy session for use in a with-statement.

    Args:
        database_dsn: DSN of the database.
        commit_on_exit: Whether to commit the session automatically on exit.

    Yields:
        An instance of a SQLAlchemy session.
    """
    engine = create_engine(database_dsn, echo=False, future=True)
    session_maker = sessionmaker(bind=engine)
    session = session_maker()

    try:
        yield session
        if commit_on_exit:
            session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db(database_dsn: str) -> None:
    """Initialize the database.

    Args:
        database_dsn: DSN of the database.
    """
    engine = create_engine(
        database_dsn, echo=True, future=True
    )  # Echo = True to get logs
    init_database(engine)

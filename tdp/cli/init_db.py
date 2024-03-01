# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import create_engine

from tdp.core.models import init_database


def init_db(database_dsn: str) -> None:
    """Initialize the database.

    Args:
        database_dsn: DSN of the database.
    """
    engine = create_engine(
        database_dsn, echo=True, future=True
    )  # Echo = True to get logs
    init_database(engine)

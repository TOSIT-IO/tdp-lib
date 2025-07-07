# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def get_engine(
    dsn: Optional[str] = None, *, env_var: str = "TDP_DATABASE_DSN"
) -> Engine:
    """Create a SQLAlchemy engine from a DSN."""
    dsn = dsn or os.getenv(env_var)
    if not dsn:
        raise ValueError(
            f"Database DSN must be provided via {env_var} environment variable."
        )
    return create_engine(dsn)


def get_session(engine: Optional[Engine] = None) -> Session:
    """Create a SQLAlchemy session from an engine."""
    engine = engine or get_engine()
    return sessionmaker(bind=engine)()

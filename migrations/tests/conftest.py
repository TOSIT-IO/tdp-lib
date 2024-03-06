# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest
from alembic.config import Config
from sqlalchemy import Engine

from migrations.basesettings import engine


@pytest.fixture(scope="session")
def alembic_config() -> Config:
    """This fixture has the alembic context setup configuration."""
    return Config("../alembic.ini")


@pytest.fixture
def alembic_engine() -> Engine:
    """This fixture is using the same SQLAlchemy engine as in env.py."""
    return engine

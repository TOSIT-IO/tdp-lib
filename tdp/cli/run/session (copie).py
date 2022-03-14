from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tdp.core.models import init_database, init_services


def path_or_inmemory(path):
    return path.absolute() if path else ":memory:"


def get_session_class(sqlite_path=None):
    if sqlite_path and not sqlite_path.exists():
        raise ValueError(
            "a sqlite path has been set, but the path does not exist, run `tdp init`"
        )
    path = path_or_inmemory(sqlite_path)
    engine = create_engine(
        f"postgresql://postgres:mypassword@localhost:5432/tdp", echo=False, future=True
    )
    session_class = sessionmaker(bind=engine)

    return session_class


def init_db(services, sqlite_path=None):
    path = path_or_inmemory(sqlite_path)
    engine = create_engine(
        f"postgresql://postgres:mypassword@localhost:5432/tdp", echo=True, future=True
    )  # Echo = True to get logs
    session_class = sessionmaker(bind=engine)
    init_database(engine)
    return init_services(session_class, services)

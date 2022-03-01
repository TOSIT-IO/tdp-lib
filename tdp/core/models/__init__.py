from sqlalchemy import select
from tdp.core.models.base import Base
from tdp.core.models.action_log import ActionLog
from tdp.core.models.deployment_log import DeploymentLog
from tdp.core.models.service import Service
from tdp.core.models.service_log import ServiceLog


def init_database(engine):
    Base.metadata.create_all(engine)


def init_services(session_class, services):
    with session_class(expire_on_commit=False) as session:
        current_services = {
            service.name for service in session.execute(select(Service)).scalars().all()
        }
        for service in set(services).difference(current_services):
            session.add(Service(name=service))
        session.commit()
        return session.execute(select(Service)).scalars().all()

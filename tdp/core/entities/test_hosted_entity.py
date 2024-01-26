from tdp.core.entities.hostable_entity_name import ServiceComponentName, ServiceName
from tdp.core.entities.hosted_entity import (
    HostedService,
    HostedServiceComponent,
    create_hosted_entity,
)


def test_hosted_service():
    hosted_service = HostedService(ServiceName("s"), "h")

    assert isinstance(hosted_service, HostedService)
    assert hosted_service.name == "s"
    assert hosted_service.host == "h"


def test_hosted_service_component():
    hosted_service = HostedServiceComponent(ServiceComponentName.from_name("s_c"), "h")

    assert isinstance(hosted_service, HostedServiceComponent)
    assert hosted_service.name == "s_c"
    assert str(hosted_service.service) == "s"
    assert str(hosted_service.component) == "c"
    assert hosted_service.host == "h"


def test_create_hosted_entity():
    hosted_service = create_hosted_entity(ServiceName("s"), "h")
    assert isinstance(hosted_service, HostedService)
    hosted_service_component = create_hosted_entity(
        ServiceComponentName.from_name("s_c"), "h"
    )
    assert isinstance(hosted_service_component, HostedServiceComponent)

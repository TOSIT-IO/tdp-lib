from tdp.core.entities.hosted_entity import HostedService, HostedServiceComponent


def test_hosted_service():
    hosted_service = HostedService("s", "h")

    assert isinstance(hosted_service, HostedService)
    assert hosted_service.name == "s"
    assert hosted_service.host == "h"


def test_hosted_service_component():
    hosted_service = HostedServiceComponent.from_name("s_c", "h")

    assert isinstance(hosted_service, HostedServiceComponent)
    assert hosted_service.name == "s_c"
    assert str(hosted_service.service) == "s"
    assert str(hosted_service.component) == "c"
    assert hosted_service.host == "h"

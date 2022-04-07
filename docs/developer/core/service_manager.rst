Service Manager
===============


A :py:class:`~tdp.core.service_manager.ServiceManager` manages, for each service,
a :ref:`repository` where the configuration for this service is saved and versioned.

The way to used it is to get a `dict` of `ServiceManager`. Each `key` of the `dict` is a `service name` and `value` is the `ServiceManager` that manages it.

For a specific service, we can get the list of components modified since a specific version. It is used to know what components should be restarted if a component configuration is updated.

Models
======

Models store classes which define the database schema.

.. _action_log:

OperationLog
------------

Each DAG node is an operation with an action. When an operation is performed and finished the results are stored.

.. _deployment_log:

DeploymentLog
-------------

A `DeploymentLog` is the result of running multiple operations and for each service the version deployed.

ServiceLog
----------

A `ServiceLog` store, for each service and for each deployment, what is the version of the service deployed.

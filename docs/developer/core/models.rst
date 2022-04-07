Models
======

Models store classes which define the database schema.

.. _action_log:

ActionLog
---------

Each DAG node is a component with an action. When an action is launched and finished the results are stored.

.. _deployment_log:

DeploymentLog
-------------

A `DeploymentLog` is the result of running multiple actions and for each service the version deployed.

ServiceLog
----------

A `ServiceLog` store, for each service and for each deployment, what is the version of the service deployed.

# TDP lib - core - models

Models store class which define the database schema.

## ActionLog

Each DAG node is a component with an action. When an action is launched and finished the 

## DeploymentLog

A `DeploymentLog` is the result of running multiple actions and for each service the version deployed.

## ServiceLog

A `ServiceLog` store, for each service and for each deployment, what is the version of the service deployed.

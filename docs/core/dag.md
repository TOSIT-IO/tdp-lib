# TDP lib - core - dag

## Dag

The `Dag` class read YAML [components](../components.md) files and validate it according to [components rules](../components.md#rules) to build the DAG.

It is used to get a list of actions by performing a topological sort on the DAG or on a subgraph of the DAG.

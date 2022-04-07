DAG
===

The `Dag` class reads YAML :doc:`../components` files
and validate it according to components rules(cf. components' rules section)
to build the DAG.

It is used to get a list of actions by performing a topological sort on the DAG
or on a subgraph of the DAG.

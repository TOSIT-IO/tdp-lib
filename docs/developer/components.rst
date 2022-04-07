Components
==========

Definition
----------

A component is composed of 3 parts: service name, component name, action.

Each service has components and each component has actions.

The format is : `<service>_<component>_<action>`.

Action
------

Each component have actions which represent a part of deployment. In order to deploy a component, all actions for this component are executed.

There are standard actions:

* `install`: used to upload and install binaries, create Unix users, etc.
* `config`: used to render configuration files for the component (XML, properties, shell env, etc.)
* `start`: used to start the component
* `init`: used to perform post-start steps

Directed acyclic graph (DAG)
----------------------------

`By definition <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_, a DAG is a graph with directed edge and no cycle.

This type of graph can be used to schedule the deployment and the configuration of components with ordering constraints by using a topological sort (or topological ordering) to produce a list of nodes which respect the ordering constraints.

Inside TDP, we use a DAG to deploy a component before another and determine which components should be restarted if a component configuration is updated.

Each node (or vertex) is a component and edges are dependencies constraints.

Dependency
----------

To build the DAG, each component define a list of dependencies, for example, before running `config`, we must perform `install` in order to have binaires and users.

Noop flag
---------

A component can have a `noop` flag to indicate that this component should be in the DAG but nothing is executed.

YAML format
-----------

YAML is used to defined components and dependencies, for example:

.. code-block:: yaml

    - name: hadoop_client_install
      depends_on: []

    - name: hadoop_client_config
      depends_on:
        - zookeeper_config
        - hadoop_client_install


Rules
-----

The components DAG follow rules:

* ``*_start`` actions can only be required from within its own service
* ``*_install`` actions should only depend on other `*_install` actions
* Each service (HDFS, HBase, Hive, etc) should have ``*_install``, ``*_config``, ``*_init`` and ``*_start`` actions even if they are "empty" (tagged with `noop`)
* Actions tagged with the `noop` flag should not have a playbook defined in the collection
* Each service action (`config`, `start`, `init`) except the first (`install`) must have an explicit dependency with the previous service action within the same service

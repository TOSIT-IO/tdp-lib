Command Line Interface
======================

TDP is a cli implemented to run actions easily

.. toctree::
 
  developer_quick_start

Usage
-----

.. code-block:: shell

  Usage: tdp [OPTIONS] COMMAND [ARGS]...

  Options:
    --help  Show this message and exit.

  Commands:
    browse            Browse deployment logs
    dag               Compute and display a graph.
    default-diff      Difference between tdp_vars and defaults
    deploy            Deploy TDP
    init              Init database / services in tdp vars
    nodes             List nodes from components DAG
    service-versions  Get the version of deployed services.(If a service has
                      never been deployed, does not show it)

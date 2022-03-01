# TDP Lib

Install tdp lib for dev:
```
poetry install # Installs dependencies and the package in venv
poetry run githooks setup
```

Install optional dependencies for graph visualization:
```
sudo apt install graphviz
poetry install -E visualization
```

Run the tests:
```
poetry run pytest tdp
```

Format the code:
```
poetry run black .
```

## Developper tools

### DAG

Dag is a custom tool installed in environment via poetry. It allows to visualize the components graph.

To run `dag` you can either use `poetry run dag` or `dag` if you are inside the poetry shell.

Dag usage:
```
usage: dag [-h] [-g | -r] [nodes [nodes ...]]

Compute and display a graph. Add node names to get a subgraph to the nodes.

positional arguments:
  nodes       Nodes on which to produce ancestors graph

optional arguments:
  -h, --help  show this help message and exit
  -g          Each node argument will be process as glob pattern
  -r          Each node argument will be process as regex pattern
```

### TDP

TDP is a development tool implemented to run actions easily

#### TDP usage

```
usage: tdp [-h] {nodes,browse,deploy,init,default_diff,services_version} ...

TDP Runner

positional arguments:
  {nodes,browse,deploy,init,default_diff,services_version}
    nodes               List nodes from components DAG
    browse              Browse deployment logs
    deploy              Deploy TDP
    init                Init database / services in tdp vars
    default_diff        Difference between tdp_vars and defaults
    services_version    Get the version of deployed services. (If a service
                        has never been deployed, does not show it

optional arguments:
  -h, --help            show this help message and exit
```


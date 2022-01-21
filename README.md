# TDP Lib

Install tdp lib for dev:
```
poetry install # Installs dependencies and the package in venv
poetry run githooks setup
```

Install optional dependencies for graph visualization:
```
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
usage: dag [-h] [nodes [nodes ...]]

Compute and display a graph. Add node names to get a subgraph to the nodes.

positional arguments:
  nodes       Nodes on which to produce ancestors graph

optional arguments:
  -h, --help  show this help message and exit
```

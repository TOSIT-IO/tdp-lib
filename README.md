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
`tdp` is the main tool with 3 commands, `nodes`, `deploy` and `browse`

```
usage: tdp [-h] {nodes,deploy,browse} ...

TDP Runner

positional arguments:
  {nodes,deploy,browse}
    nodes               List nodes from components DAG
    deploy              Deploy's help
    browse              Browse's help

optional arguments:
  -h, --help            show this help message and exit
```

#### Node usage
```
usage: tdp nodes [-h]

optional arguments:
  -h, --help  show this help message and exit
```

#### Deploy usage
```
usage: tdp deploy [-h] [--playbooks_directory PLAYBOOKS_DIRECTORY]
                  [--run-directory RUN_DIRECTORY] [--sqlite-path SQLITE_PATH]
                  [--filter FILTER] [--dry]
                  [target]

positional arguments:
  target                Node in the dag, if no target is specified, all the
                        nodes (minus the filter) are selected

optional arguments:
  -h, --help            show this help message and exit
  --playbooks_directory PLAYBOOKS_DIRECTORY
                        Path to tdp-collection playbooks, settable through
                        `TDP_PLAYBOOKS_DIRECTORY` environment variable
  --run-directory RUN_DIRECTORY
                        Working binary where the executor is launched
                        (`ansible-playbook` for Ansible), settable through
                        `TDP_RUN_DIRECTORY` environment variable
  --sqlite-path SQLITE_PATH
                        Path to SQLITE database file, settable through
                        `TDP_SQLITE_PATH` environment variable
  --filter FILTER       Glob on list name
  --dry                 Execute dag without running any action
```

#### Browse usage

```
usage: tdp browse [-h] [--sqlite-path SQLITE_PATH] [deployment_id] [action]

positional arguments:
  deployment_id         Deployment to display
  action                Action to display

optional arguments:
  -h, --help            show this help message and exit
  --sqlite-path SQLITE_PATH
                        Path to SQLITE database file, settable through
                        `TDP_SQLITE_PATH` environment variable
```

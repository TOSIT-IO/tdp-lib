[![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# TDP Lib

TDP lib is a [Python](https://www.python.org/) library built on top of [Ansible](https://www.ansible.com/) to manage clusters. It provides a set of tools to overcome the limitations of Ansible, such as:

- Defining a DAG of tasks based on relations between services and components
- Defining variables in a single place

`tdp-lib` can be used as a Python library, through an admin [CLI](#cli-usage), a REST API (see [`tdp-server`](https://github.com/tOSIT-IO/tdp-server)) or a web interface (see [`tdp-ui`](https://github.com/tOSIT-IO/tdp-ui)).

## Pre-requisites

TDP lib requires:

- Python 3.9+ with [Poetry](https://python-poetry.org/)
- A RDBMS system (such as [PostgreSQL](https://www.postgresql.org/) or [SQLite](https://www.sqlite.org/index.html))

Optionally, you can install the following dependencies for DAG visualization:

- [graphviz](https://graphviz.org/)
- Python `visualization` dependency (`poetry install -E visualization`)

And to build the documentation:

- Python `docs` dependency (`poetry install -E docs`)

## Installation

Install dependencies and the package in a virtual environment:

```sh
poetry install
```

Export the following environment variables:

- `TDP_COLLECTION_PATH`: path(s) to the collection(s). [`tdp-collection`](https://github.com/TOSIT-IO/tdp-collection) is mandatory. Other collections can be added, separated by a colon `:` (such as [`tdp-collection-extras`](https://github.com/TOSIT-IO/tdp-collection-extras), [`tdp-observability`](https://github.com/TOSIT-IO/tdp-observability)).
- `TDP_RUN_DIRECTORY`: path to the working directory of TDP deployment (where `ansible.cfg`, `inventory.ini` and `topology.ini` are located).
- `TDP_DATABASE_DSN`: DSN of the database to use.
- `TDP_VARS`: path to the folder containing the variables.

Note: Ansible must be configured to use the `tosit.tdp.inventory` plugin. For example, in `ansible.cfg`:

```ini
[defaults]
inventory=your_inventory,..,~/tdp_vars

[inventory]
enable_plugins = tosit.tdp.inventory,..,your_plugins
```

Finally, initialize the database and default variables:

```sh
poetry shell
tdp init
```

## CLI usage

Full documentation can be found inside [docs/cli](docs/developer/cli/index.rst).

### Build the documentation

Documentation can be built with:

```sh
poetry run task docs-html
```

Built doc is available at `docs/_build/html/index.html`.

## Contributing

Install pre-commit hooks:

```sh
poetry run pre-commit install --hook-type pre-commit
poetry run pre-commit install --hook-type commit-msg
```

Run the tests:

```sh
poetry run pytest tdp
```

Format the code:

```sh
poetry run black .
```

Developers documentation: [docs/Developer](docs/developer/index.rst)

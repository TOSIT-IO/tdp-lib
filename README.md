[![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# TDP Lib

`tdp-lib` is a [Python](https://www.python.org/) library built on top of [Ansible](https://www.ansible.com/). It is designed for managing TDP clusters, offering advanced tools to extend Ansible's capabilities. Key features include:

- Creating a Directed Acyclic Graph (DAG) of tasks to manage dependencies and relationships between services and components.
- Centralized configuration management through a unified variable definition system.

`tdp-lib` can be utilized in various ways: as a Python library, through an admin [CLI](#cli-usage), via a REST API (see [`tdp-server`](https://github.com/TOSIT-IO/tdp-server)), or through a web interface (see [`tdp-ui`](https://github.com/TOSIT-IO/tdp-ui)).

## Pre-requisites

To use `tdp-lib`, ensure you have the following prerequisites:

- Python 3.9 or higher.
- A relational database management system (RDBMS), such as [PostgreSQL](https://www.postgresql.org/) or [SQLite](https://www.sqlite.org/index.html).

Optional dependencies for DAG visualization:

- [Graphviz](https://graphviz.org/) for graphical representation of DAGs.

## Installation

Set the following environment variables:

- `TDP_COLLECTION_PATH`: Specifies the file path(s) to the necessary collection(s). [`tdp-collection`](https://github.com/TOSIT-IO/tdp-collection) is mandatory. Multiple collections can be specified, separated by a colon `:` (e.g., [`tdp-collection-extras`](https://github.com/TOSIT-IO/tdp-collection-extras), [`tdp-observability`](https://github.com/TOSIT-IO/tdp-observability)).
- `TDP_RUN_DIRECTORY`: Path to the working directory of the TDP deployment (where `ansible.cfg`, `inventory.ini`, and `topology.ini` are located).
- `TDP_DATABASE_DSN`: Database DSN (Data Source Name) for the chosen RDBMS.
- `TDP_VARS`: Path to the folder containing configuration variables.

Ensure Ansible is configured to use the `tosit.tdp.inventory` plugin. Example `ansible.cfg`:

```ini
[defaults]
inventory=your_inventory,..,~/tdp_vars

[inventory]
enable_plugins = tosit.tdp.inventory,..,your_plugins
```

Install the library:

```sh
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate
# Install the dependencies
pip install ".[visualization]"
# Initialize the database and tdp_vars
tdp init
```

## CLI Usage

> [!NOTE]
> This section is a work in progress.

```sh
tdp --help
```

## Contributing

Contributions are welcome! Here are some guidelines specific to this project:

- Use [Poetry](https://python-poetry.org/) for development:

    ```sh
    # Install Poetry
    curl -sSL https://install.python-poetry.org | python3 -
    # Install the dependencies
    poetry install
    ```

- Commit messages must adhere to the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) standard.
- Run tests before submitting a PR:

    ```sh
    poetry run pytest tdp
    ```

- Format and lint code using ([Black](https://black.readthedocs.io/en/stable/)) and ([Ruff](https://beta.ruff.rs/docs/)):

    ```sh
    poetry run task precommit-fix
    ```

### Developer Documentation

Developer documentation is available here: [docs/Developer](docs/developer/index.rst)

Docstrings are used to generate Sphinx documentation. Install the `docs`` extra dependency:

```sh
poetry install -E docs
```

Build the documentation:

```sh
poetry run task docs
```

The built documentation is available at `docs/_build/html/index.html`.

[![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# TDP Lib

`tdp-lib` is a [Python](https://www.python.org/) library built on top of [Ansible](https://www.ansible.com/). It is designed for managing TDP clusters, offering advanced tools to extend Ansible's capabilities. Key features include:

- Creating a Directed Acyclic Graph (DAG) of tasks to manage dependencies and relationships between services and components.
- Centralized configuration management through a unified variable definition system.

`tdp-lib` can be utilized in various ways: as a Python library, through an admin [CLI](#cli-usage), via a REST API (see [`tdp-server`](https://github.com/TOSIT-IO/tdp-server)), or through a web interface (see [`tdp-ui`](https://github.com/TOSIT-IO/tdp-ui)).

## Pre-requisites

To use `tdp-lib`, ensure you have the following prerequisites:

- Python 3.9 or higher. Higher version are not guarantee to work, only 3.9 is tested.
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
pip install "tdp-lib[visualization]@https://github.com/TOSIT-IO/tdp-lib/tarball/1.0.0"
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
    poetry install -E postgresql-binary -E mysql
    ```

- Commit messages must adhere to the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) standard.
- Run tests on all supported databases before submitting a PR:

    ```sh
    docker compose -f dev/docker-compose.yaml up -d
    poetry run pytest tests --database-dsn 'postgresql+psycopg2://postgres:postgres@localhost:5432/tdp' --database-dsn 'mysql+pymysql://mysql:mysql@localhost:3306/tdp' --database-dsn 'mysql+pymysql://mariadb:mariadb@localhost:3307/tdp'
    docker compose -f dev/docker-compose.yaml down -v
    ```

- Format and lint code using [Ruff](https://beta.ruff.rs/docs/):

    ```sh
    # Format the code and reoder imports
    poetry run ruff check --select I --fix && ruff format
    # Check the code for linting issues
    poetry run ruff check
    # Lint the code
    poetry run ruff check --fix
    ```

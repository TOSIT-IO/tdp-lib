[![Python Version](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# TDP Lib

Install tdp lib for dev:
```
poetry install # Installs dependencies and the package in venv
poetry run pre-commit install --hook-type pre-commit
poetry run pre-commit install --hook-type commit-msg
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

## Documentation

Documentation can be found inside:

* [docs](docs/developer/index.rst) for documentation index
* [docs/cli](docs/developer/cli/index.rst) for CLI documentation index
* [docs/Developer Quick Start](docs/developer/cli/developer_quick_start.rst)

To generate the documentation:

```
poetry install -E docs
poetry run task docs-html
```

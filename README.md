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

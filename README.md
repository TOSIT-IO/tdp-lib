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

## Cli
### TDP

TDP is a cli implemented to run actions easily

#### TDP usage

```
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
```


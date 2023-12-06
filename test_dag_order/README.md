# Test reconfigure DAG order

This folder contains some utilities to test the DAG order of a TDP collection.

This utility is based on [`pytest`](https://docs.pytest.org/en/stable/) test generation feature. Tests are generated from a set of base tests defined in the `test_dag_order.py` file for each rule defined in the `tdp_lib_dag_test_rules` folder of the TDP collection.

## Usage

```bash
pytest test_dag_order [pytest_options] --collection-path <path_to_tdp_collection>
```

Where `--collection-path` is the path to the TDP collection to test. It can be used multiple times.

For example, to test the `tdp` collection:

```bash
poetry run pytest -n 12 --show-capture=no --traceback=short test_dag_order --collection-path /home/paul/code/tosit/getting-started/ansible_collections/tosit/tdp
```

Recommended options are:

- `-n`: number of parallel tests to run
- `--show-capture`: show the output of the tests
- `--traceback`: show the traceback of the tests

## Rule discovery

Every `.yml` file placed in the `tdp_lib_dag_test_rules` of a TDP collection can contain a list of rules to test. Each rule is a dictionary where:

- the key is the `source` component (or service) to reconfigure.
- the properties are:
  - `must_include`: a list of components (or services) that must be reconfigured
  - `must_exclude`: a list of components (or services) that must not be reconfigured


For example, the following rule will test that editing the `hdfs.yml` configuration will trigger a reconfigure that must include the `yarn` service and must exclude the `zookeeper` service:

```yaml
hdfs:
  must_include:
    - yarn
  must_exclude:
    - zookeeper
```

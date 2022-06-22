Runners
=======

Runners are used to run operations with the DAG ordering constraints.

To use a runner, an :py:class:`~tdp.core.runner.operation_runner.OperationRunner` must be instanciated.
In order to work, it needs a :py:class:`~tdp.core.dag.Dag` instance to run
operations with the DAG ordering constraints, an implementation of `Executor` to run each operation,
a `ServiceManager` `dict` to know the service version
to deploy needed to build a :ref:`deployment_log`.

Executor
--------

:py:class:`~tdp.core.runner.executor.Executor` define the interface to run an operation.

AnsibleExecutor
---------------

:py:class:`~tdp.core.runner.ansible_executor.AnsibleExecutor` is an implementation for :py:class:`~tdp.core.runner.executor.Executor`
in order to run operations with Ansible. The operation name is the Ansible playbook to execute with `ansible-playbook` command.

OperationRunner
---------------

:py:class:`~tdp.core.runner.operation_runner.OperationRunner` uses :py:class:`~tdp.core.runner.executor.Executor` implementations to run one operation
or run multiple opertaions with the DAG ordering constraints.

Runners
=======

Runners are used to run operations with the DAG ordering constraints.

To use a runner, an :py:class:`~tdp.core.deployment.DeploymentRunner` must be instanciated.
In order to work, it needs a :py:class:`~tdp.core.collections.Collections` instance to be able to translate
an operation into its action file, an implementation of `Executor` to run each operation,
a `~tdp.core.variables.ClusterVariables` to know the service version
deployed needed to build a :ref:`deployment_log`.

Executor
--------

:py:class:`~tdp.core.deployment.Executor` define the interface to run an operation.

AnsibleExecutor
---------------

:py:class:`~tdp.core.deployment.AnsibleExecutor` is an implementation for :py:class:`~tdp.core.deployment.Executor`
in order to run operations with Ansible. The operation name is the Ansible playbook to execute with `ansible-playbook` command.

DeploymentRunner
----------------

:py:class:`~tdp.core.deployment.DeploymentRunner` uses :py:class:`~tdp.core.deployment.Executor` implementations to run one operation
or run multiple operations defined in a `~tdp.core.deployment.DeploymentPlan`.

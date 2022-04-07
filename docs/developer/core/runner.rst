Runners
=======

Runners are used to run actions with the DAG ordering constraints.

To use a runner, an :py:class:`~tdp.core.runner.action_runner.ActionRunner` must be instanciated.
In order to work, it needs a :py:class:`~tdp.core.dag.Dag` instance to run
actions with the DAG ordering constraints,an implementation of `Executor` to run each actions,
a `ServiceManager` `dict` to know the service version
to deploy needed to build a :ref:`deployment_log`.

Executor
--------

:py:class:`~tdp.core.runner.executor.Executor` define the interface to run an action.

AnsibleExecutor
---------------

:py:class:`~tdp.core.runner.ansible_executor.AnsibleExecutor` is an implementation for :py:class:`~tdp.core.runner.executor.Executor`
in order to run action with Ansible. The action name is the Ansible playbook to execute with `ansible-playbook` command.

ActionRunner
------------

:py:class:`~tdp.core.runner.action_runner.ActionRunner` uses :py:class:`~tdp.core.runner.executor.Executor` implementations to run one action 
or run multiple action with the DAG ordering constraints.

If one action is run, only an :ref:`action_log` is returned, if multiple actions are run, a :ref:`deployment_log` is returned.

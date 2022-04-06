# TDP lib - core - runner

Runners are used to run actions with the DAG ordering constraints.

To use a runner, an `ActionRunner` must be instanciate. In order to work, it needs a [Dag](dag.md) instance to run actions with the DAG ordering constraints, an implementation of `Executor` to run each actions, a `ServiceManager` `dict` to know the service version to deploy needed to build a [DeploymentLog](models.md#deploymentlog).

## Executor

`Executor` define the interface to run an action.

## AnsibleExecutor

`AnsibleExecutor` is an implementation for `Executor` in order to run action with Ansible. The action name is the Ansible playbook to execute with `ansible-playbook` command.

## ActionRunner

`ActionRunner` use `Executor` implementation to run one action or run multiple action with the DAG ordering constraints.

If one action is run, only an [ActionLog](models.md#actionlog) is returned, if multiple actions is run, a [DeploymentLog](models.md#deploymentlog) is returned.

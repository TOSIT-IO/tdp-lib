import click
from tdp.core.models import OperationLog


def forceCheck(force, planned_deployment):
    if planned_deployment and not force:
        click.echo(
            f"A planned deployment with id {planned_deployment.id} exists, use `--force` option to modify it"
        )
        return False
    elif planned_deployment and force:
        click.echo(f"Modifying a planned deployment with id {planned_deployment.id}")
    return True

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING

import click

from tdp.cli.params import database_dsn_option

if TYPE_CHECKING:
    from sqlalchemy import Engine


@click.command("danger-fix-running")
@database_dsn_option
def danger_fix_running(
    db_engine: Engine,
):
    """Fix the last deployment state if left as RUNNING.

    DANGER: Only use this command if the database has been left in an incorect state,
    where the last deployment state is 'RUNNING' while it is not the case. Ensure
    that no Ansible deployment is indeed running.

    This command will only override the last deployment state to set it as 'FAILURE'.
    No Ansible command will be executed.
    """

    from tdp.core.models.deployment_model import NothingToFixError
    from tdp.dao import Dao

    with Dao(db_engine) as dao:
        last_deployment = dao.get_last_deployment()
        if last_deployment is None:
            raise click.ClickException("No deployment found.")

        try:
            last_deployment.fix_running()
        except NothingToFixError:
            raise click.ClickException(
                "Nothing to fix: last deployment is not in a RUNNING state."
            )
        else:
            dao.session.commit()
            click.echo(
                "Last deployement has been succesfully set to FAILURE. Use "
                "'tdp plan resume' to generate a new deployment plan based on it."
            )

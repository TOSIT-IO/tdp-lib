from pathlib import Path

from sqlalchemy import select
from tabulate import tabulate

from tdp.core.models import ActionLog, DeploymentLog, ServiceLog
from tdp.core.models.base import keyvalgen
from tdp.cli.run.commands.command import Command
from tdp.cli.run.env_default import EnvDefault
from tdp.cli.run.session import get_session_class


class BrowseCommand(Command):
    """Browse deployment logs"""

    def __init__(self, adapter, args, dag) -> None:
        super().__init__(adapter, args, dag)
        self.limit = args.limit
        self.offset = args.offset
        self.deployment_id = args.deployment_id
        self.action = args.action
        self.sqlite_path = args.sqlite_path

    def run(self):
        if self.sqlite_path is None:
            raise ValueError("SQLITE_PATH cannot be None")
        session_class = get_session_class(self.sqlite_path)

        if not self.deployment_id:
            self.process_deployments_query(session_class)
        else:
            if not self.action:
                self.process_single_deployment_query(session_class)
            else:
                self.process_action_query(session_class)

    # ----------- Process query functions

    def process_deployments_query(self, session_class):
        headers = [key for key, _ in keyvalgen(DeploymentLog)]
        query = (
            select(DeploymentLog)
            .order_by(DeploymentLog.id)
            .limit(self.limit)
            .offset(self.offset)
        )

        with session_class() as session:
            result = session.execute(query).scalars().fetchall()
            self.adapter.info(
                "Deployments:\n"
                + tabulate(
                    [
                        format_deployment_log(deployment_log, headers)
                        for deployment_log in result
                    ],
                    headers="keys",
                )
            )

    def process_single_deployment_query(self, session_class):
        deployment_headers = [key for key, _ in keyvalgen(DeploymentLog)]
        action_headers = [key for key, _ in keyvalgen(ActionLog)]
        service_headers = [
            key for key, _ in keyvalgen(ServiceLog) if key != "deployment"
        ]
        action_headers.remove("deployment")
        query = (
            select(DeploymentLog)
            .where(DeploymentLog.id == self.deployment_id)
            .order_by(DeploymentLog.id)
        )

        with session_class() as session:
            result = session.execute(query).scalars().fetchall()
            self.adapter.info(
                "Deployment:\n"
                + tabulate(
                    [
                        format_deployment_log(deployment_log, deployment_headers)
                        for deployment_log in result
                    ],
                    headers="keys",
                )
            )
            self.adapter.info(
                "Services:\n"
                + tabulate(
                    [
                        format_service_log(service_logs, service_headers)
                        for deployment_log in result
                        for service_logs in deployment_log.services
                    ],
                    headers="keys",
                )
            )
            self.adapter.info(
                "Actions:\n"
                + tabulate(
                    [
                        format_action_log(action_log, action_headers)
                        for action_log in result[0].actions
                    ],
                    headers="keys",
                )
            )

    def process_action_query(self, session_class):
        headers = [key for key, _ in keyvalgen(ActionLog) if key != "deployment"]
        service_headers = [
            key for key, _ in keyvalgen(ServiceLog) if key != "deployment"
        ]
        query = (
            select(ActionLog)
            .where(ActionLog.deployment_id == self.deployment_id)
            .where(ActionLog.action == self.action)
            .order_by(ActionLog.start)
        )
        with session_class() as session:
            result = session.execute(query).scalars().fetchall()
            action_logs = [action_log for action_log in result]
            self.adapter.info(
                "Service:\n"
                + tabulate(
                    [
                        format_service_log(service_log, service_headers)
                        for action_log in result
                        for service_log in action_log.deployment.services
                        if service_log.service == action_log.action.split("_")[0]
                    ],
                    headers="keys",
                )
            )
            self.adapter.info(
                "Action:\n"
                + tabulate(
                    [
                        format_action_log(action_log, headers)
                        for action_log in action_logs
                    ],
                    headers="keys",
                )
            )
            if action_logs:
                action_log = action_logs[0]
                self.adapter.info(
                    f"{action_log.action} logs:\n" + str(action_log.logs, "utf-8")
                )

    @staticmethod
    def fill_argument_definition(parser, dag):
        parser.set_defaults(command=BrowseCommand)
        parser.add_argument(
            "deployment_id",
            nargs="?",
            type=int,
            default=None,
            help="Deployment to display",
        )
        parser.add_argument(
            "action", nargs="?", type=str, default=None, help="Action to display"
        )
        parser.add_argument(
            "--sqlite-path",
            action=EnvDefault,
            envvar="TDP_SQLITE_PATH",
            type=Path,
            default=None,
            help="Path to SQLITE database file",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=15,
            help="Limit number of deployments returned",
        )
        parser.add_argument(
            "--offset",
            type=int,
            default=0,
            help="At which offset should the database query should start",
        )


# ----------- Helper functionss


def format_deployment_log(deployment_log, headers):
    def custom_format(key, value):
        if key == "actions":
            if len(value) > 2:
                return value[0].action + ",...," + value[-1].action
            else:
                return ",".join(action.action for action in value)
        elif key == "services":
            return ",".join(str(service_log.service) for service_log in value)
        else:
            return str(value)

    return {key: custom_format(key, getattr(deployment_log, key)) for key in headers}


def format_action_log(action_log, headers):
    def custom_format(key, value):
        if key == "logs":
            return str(value[:40])
        else:
            return str(value)

    return {key: custom_format(key, getattr(action_log, key)) for key in headers}


def format_service_log(service_log, headers):
    def custom_format(key, value):
        if key == "version":
            return str(value[:7])
        else:
            return str(value)

    return {key: custom_format(key, getattr(service_log, key)) for key in headers}

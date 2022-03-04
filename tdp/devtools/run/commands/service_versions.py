from pathlib import Path

from tabulate import tabulate

from tdp.core.models import Service, ServiceLog
from tdp.core.models.base import keyvalgen
from tdp.devtools.run.commands.command import Command
from tdp.devtools.run.env_default import EnvDefault
from tdp.devtools.run.session import get_session_class


class ServicesVersionCommand(Command):
    """Get the version of deployed services. (If a service has never been deployed, does not show it)"""

    def __init__(self, adapter, args, dag) -> None:
        super().__init__(adapter, args, dag)
        self.sqlite_path = args.sqlite_path

    def run(self):
        if self.sqlite_path is None:
            raise ValueError("SQLITE_PATH cannot be None")
        session_class = get_session_class(self.sqlite_path)
        with session_class() as session:
            service_headers = [
                key for key, _ in keyvalgen(ServiceLog) if key != "deployment"
            ]

            service_latest_version = (
                session.query(ServiceLog)
                .group_by(ServiceLog.service_id)
                .order_by(ServiceLog.deployment_id.desc())
                .all()
            )

            self.adapter.info(
                "Service versions:\n"
                + tabulate(
                    [
                        format_service_log(service_log, service_headers)
                        for service_log in service_latest_version
                    ],
                    headers="keys",
                )
            )

    @staticmethod
    def fill_argument_definition(parser, dag):
        parser.set_defaults(command=ServicesVersionCommand)
        parser.add_argument(
            "--sqlite-path",
            action=EnvDefault,
            envvar="TDP_SQLITE_PATH",
            type=Path,
            default=None,
            help="Path to SQLITE database file",
        )


def format_service_log(service_log, headers):
    def custom_format(key, value):
        if key == "version":
            return str(value[:7])
        elif key == "service":
            return value.name
        else:
            return str(value)

    return {key: custom_format(key, getattr(service_log, key)) for key in headers}

from pathlib import Path
from tdp.core.repository.repository import NoVersionYet
from tdp.core.service_manager import ServiceManager
from tdp.cli.run.commands.command import Command
from tdp.cli.run.env_default import EnvDefault
from tdp.cli.run.session import init_db


class InitCommand(Command):
    """Init database / services in tdp vars"""

    def __init__(self, adapter, args, dag) -> None:
        super().__init__(adapter, args, dag)
        self.sqlite_path = args.sqlite_path
        self.collection_path = args.collection_path
        self.vars = args.vars

    def run(self):
        if self.sqlite_path is None:
            raise ValueError("SQLITE_PATH cannot be None")
        init_db(self.sqlite_path)
        services = self.dag.services
        default_vars = self.collection_path / "tdp_vars_defaults"
        service_managers = ServiceManager.initialize_service_managers(
            services, self.vars, default_vars
        )
        for name, service_manager in service_managers.items():
            try:
                self.adapter.info(f"{name}: {service_manager.version}")
            except NoVersionYet:
                self.adapter.info(f"Initializing {name}")
                service_manager.initiliaze_variables(service_manager)

    @staticmethod
    def fill_argument_definition(parser, dag):
        parser.set_defaults(command=InitCommand)
        parser.add_argument(
            "--sqlite-path",
            action=EnvDefault,
            envvar="TDP_SQLITE_PATH",
            type=Path,
            default=None,
            help="Path to SQLITE database file",
        )
        parser.add_argument(
            "--collection-path",
            action=EnvDefault,
            envvar="TDP_COLLECTION_PATH",
            type=Path,
            default=None,
            help="Path to tdp-collection",
        )
        parser.add_argument(
            "--vars",
            action=EnvDefault,
            envvar="TDP_VARS",
            type=Path,
            default=None,
            help="Path to the tdp vars",
        )

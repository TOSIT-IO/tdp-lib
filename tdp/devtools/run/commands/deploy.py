from pathlib import Path

from tdp.core.runner.action_runner import ActionRunner
from tdp.core.runner.ansible_executor import AnsibleExecutor
from tdp.core.service_manager import ServiceManager

from tdp.devtools.run.commands.command import Command
from tdp.devtools.run.env_default import EnvDefault
from tdp.devtools.run.session import get_session_class


class DeployCommand(Command):
    """Deploy TDP"""

    def __init__(self, adapter, args, dag) -> None:
        super().__init__(adapter, args, dag)
        self.playbooks_directory = args.collection_path / "playbooks"
        self.target = args.target
        self.run_directory = (
            args.run_directory.absolute() if args.run_directory else None
        )
        self.vars = args.vars
        self.filter = args.filter
        self.sqlite_path = args.sqlite_path
        self.dry = args.dry

    def run(self):
        ansible_executor = AnsibleExecutor(
            playbooks_directory=self.playbooks_directory,
            run_directory=self.run_directory,
            dry=self.dry,
        )
        session_class = get_session_class(self.sqlite_path)
        with session_class() as session:
            service_managers = ServiceManager.get_service_managers(
                self.dag.services, self.vars
            )
            self.check_services_cleanliness(service_managers)

            action_runner = ActionRunner(self.dag, ansible_executor, service_managers)
            self.adapter.info(f"Deploying {self.target}")
            deployment = action_runner.run_to_node(self.target, self.filter)
            session.add(deployment)
            session.commit()

    def check_services_cleanliness(self, service_managers):
        unclean_services = [
            service_manager.name
            for service_manager in service_managers.values()
            if not service_manager.clean
        ]
        if unclean_services:
            for name in unclean_services:
                self.adapter.info(f"{name} repository is not in a clean state")
            raise ValueError("Some services are in a dirty state")

    @staticmethod
    def fill_argument_definition(parser, dag):
        parser.set_defaults(command=DeployCommand)
        if not dag:
            raise ValueError("You need to fill in the dag")

        def node(target=None):
            if target and target in dag.get_all_actions():
                return target
            raise ValueError(f"{target} is not a valid node")

        parser.add_argument(
            "target",
            nargs="?",
            type=node,
            default=None,
            help=(
                "Node in the dag, if no target is specified,"
                " all the nodes (minus the filter) are selected"
            ),
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
            "--run-directory",
            action=EnvDefault,
            envvar="TDP_RUN_DIRECTORY",
            type=Path,
            default=None,
            help="Working binary where the executor is launched (`ansible-playbook` for Ansible)",
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
            "--vars",
            action=EnvDefault,
            envvar="TDP_VARS",
            type=Path,
            default=None,
            help="Path to the tdp vars",
        )
        parser.add_argument(
            "--filter",
            type=str,
            default=None,
            help="Glob on list name",
        )
        parser.add_argument(
            "--dry", action="store_true", help="Execute dag without running any action"
        )

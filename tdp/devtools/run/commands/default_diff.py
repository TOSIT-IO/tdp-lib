import difflib
import os
from pathlib import Path
from tdp.core.models.service import Service
from tdp.core.service_manager import ServiceManager
from tdp.core.variables import Variables
from tdp.devtools.run.colors import END, GREEN, RED, YELLOW
from tdp.devtools.run.commands.command import Command
from tdp.devtools.run.env_default import EnvDefault

import yaml
import pprint


class DefaultDiffCommand(Command):
    """Difference between tdp_vars and defaults"""

    def __init__(self, adapter, args, dag) -> None:
        super().__init__(adapter, args, dag)
        self.collection_path = args.collection_path
        self.vars = args.vars
        self.service = args.service

    def run(self):
        services = [Service(name=service) for service in self.dag.services]
        service_managers = ServiceManager.get_service_managers(services, self.vars)
        collection_default_vars = self.collection_path / "tdp_vars_defaults"

        if self.service:
            self.service_diff(collection_default_vars, service_managers[self.service])
        else:
            for service in self.dag.services:
                self.service_diff(collection_default_vars, service_managers[service])

    def service_diff(self, collection_default_vars, service):
        """computes the difference between the default variables from a service, and the variables from your service variables inside your tdp_vars

        Args:
            collection_default_vars (Path): default vars path
            service (ServiceManager): service to compare's manager
        """
        default_service_vars = collection_default_vars / service.name
        # key: filename with extension, value: PosixPath(filepath)
        default_service_vars_paths = {
            path.name: path for path in default_service_vars.glob("*.yml")
        }
        for (
            default_service_vars_filename,
            default_service_vars_filepath,
        ) in default_service_vars_paths.items():
            tdp_vars_service_vars_filepath = (
                service.path / default_service_vars_filename
            )
            if not tdp_vars_service_vars_filepath.exists():
                self.adapter.info(
                    f"{service.name}: {default_service_vars_filename}\n"
                    f"{RED}{tdp_vars_service_vars_filepath} does not exist{END}"
                )
                continue

            with default_service_vars_filepath.open(
                "r"
            ) as default_service_varfile, tdp_vars_service_vars_filepath.open(
                "r"
            ) as service_varfile:
                default_service_varfile_content = pprint.pformat(
                    yaml.safe_load(default_service_varfile)
                ).splitlines()
                service_varfile_content = pprint.pformat(
                    yaml.safe_load(service_varfile)
                ).splitlines()
                # left_path = tdp_vars_defaults/{service}/{filename}
                left_path = str(
                    default_service_vars_filepath.relative_to(
                        collection_default_vars.parent
                    )
                )
                # right_path = {your_tdp_vars}/{service}/{filename}
                right_path = str(
                    tdp_vars_service_vars_filepath.relative_to(
                        service.path.parent.parent
                    )
                )
                self.compute_and_print_difference(
                    service_name=service.name,
                    left_content=default_service_varfile_content,
                    right_content=service_varfile_content,
                    left_path=left_path,
                    right_path=right_path,
                    filename=default_service_vars_filename,
                )

    def compute_and_print_difference(
        self, service_name, filename, left_content, right_content, left_path, right_path
    ):
        """computes differences between 2 files, and outputs them.

        Args:
            service_name (str): name of the service
            filename (str): name of the file to display
            left_content (Iterator[str]): content to compare from
            right_content (Iterator[str]): content to compare to
            left_path (str): filename to compare from, to use as contextual information
            right_path (str): filename to compare to, to use as contextual information
        """
        differences = difflib.context_diff(
            left_content,
            right_content,
            fromfile=left_path,
            tofile=right_path,
            n=1,
        )
        self.adapter.info(
            f"{service_name}: {filename}\n"
            + ("\n".join(color_line(line) for line in differences) or "None")
        )

    @staticmethod
    def fill_argument_definition(parser, dag):
        parser.set_defaults(command=DefaultDiffCommand)
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
        parser.add_argument(
            "service",
            nargs="?",
            type=str,
            choices=dag.services,
            default=None,
            help="Service to compare",
        )


def color_line(line: str):
    if line.startswith("! "):
        return YELLOW + line + END
    if line.startswith("- "):
        return RED + line + END
    if line.startswith("+ "):
        return GREEN + line + END

    return line

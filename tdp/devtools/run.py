import argparse
import logging
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tdp.core.dag import Dag
from tdp.core.models import Base
from tdp.core.runner.action_runner import ActionRunner
from tdp.core.runner.ansible_executor import AnsibleExecutor

logger = logging.getLogger("tdp").getChild("dag")

DAG = Dag()

# ----------- Argument definition


def arguments_definition():
    """
    Inputs:

    * playbooks_directory
    * target (node in the dag)
    * run_directory (optional, run in current dir)
    * path/to/sqlite.db (optional, save only if path is provided)
    * filter (optional, regex ? glob ?)
    * dry run (display playbooks that would have been run)

    Outputs:

    * playbooks stdout and stderr or commands in dry run mode

    """
    parser = argparse.ArgumentParser(description="TDP Runner")
    subparsers = parser.add_subparsers()
    node_parser = subparsers.add_parser("nodes", help="List nodes from components DAG")
    node_parser.add_argument("--list-nodes", default=True, help="List all nodes in dag")
    run_parser = subparsers.add_parser("deploy", help="Deploy's help")
    run_parser.add_argument(
        "target",
        nargs="?",
        type=node,
        default=None,
        help=(
            "Node in the dag, if no target is specified,"
            " all the nodes (minus the filter) are selected"
        ),
    )
    run_parser.add_argument(
        "--playbooks_directory",
        action=EnvDefault,
        envvar="TDP_PLAYBOOKS_DIRECTORY",
        type=Path,
        default=None,
        help="Path to tdp-collection playbooks",
    )
    run_parser.add_argument(
        "--run-directory",
        action=EnvDefault,
        envvar="TDP_RUN_DIRECTORY",
        type=Path,
        default=None,
        help="Working binary where the executor is launched (`ansible-playbook` for Ansible)",
    )
    run_parser.add_argument(
        "--sqlite-path",
        action=EnvDefault,
        envvar="TDP_SQLITE_PATH",
        type=Path,
        default=None,
        help="Path to SQLITE database file",
    )
    run_parser.add_argument(
        "--filter",
        type=str,
        default=None,
        help="Glob on list name",
    )
    run_parser.add_argument(
        "--dry", action="store_true", help="Execute dag without running any action"
    )
    return parser


class EnvDefault(argparse.Action):
    # Inspired greatly from https://stackoverflow.com/a/10551190
    def __init__(self, envvar, required=True, default=None, help=None, **kwargs):
        if not default and envvar:
            if envvar in os.environ:
                default = os.environ[envvar]
            if help:
                help += f", settable through `{envvar}` environment variable"
        if required and default:
            required = False
        super(EnvDefault, self).__init__(
            default=default, required=required, help=help, **kwargs
        )

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


# ----------- Helper functions


def node(target=None):
    if target and target in DAG.get_all_actions():
        return target
    raise ValueError(f"{target} is not a valid node")


def get_session_class(sqlite_path=None):
    path = sqlite_path.absolute() if sqlite_path else ":memory:"
    engine = create_engine(f"sqlite+pysqlite:///{path}", echo=False, future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


# ----------- Commands


def list_nodes():
    endline = "\n- "
    components = endline.join(component for component in DAG.get_all_actions())
    logger.info(f"Component list:{endline}{components}")


def deploy_action(
    playbooks_directory,
    target=None,
    run_directory=None,
    filter=None,
    sqlite_path=None,
    dry=False,
):
    run_directory = run_directory.absolute() if run_directory else None
    ansible_executor = AnsibleExecutor(
        playbooks_directory=playbooks_directory, run_directory=run_directory, dry=dry
    )
    action_runner = ActionRunner(DAG, ansible_executor)
    session_class = get_session_class(sqlite_path)
    with session_class() as session:
        deployment = action_runner.run_to_node(target, filter)
        session.add(deployment)
        session.commit()


# ----------- Main


def main():
    args = arguments_definition().parse_args()
    logger.debug("Arguments: " + str(vars(args)))
    if "list_nodes" in args:
        list_nodes()
    else:
        deploy_action(
            playbooks_directory=args.playbooks_directory,
            target=args.target,
            run_directory=args.run_directory,
            filter=args.filter,
            sqlite_path=args.sqlite_path,
            dry=args.dry,
        )

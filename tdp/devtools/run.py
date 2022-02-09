import argparse
import logging
import os
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from tabulate import tabulate

from tdp.core.dag import Dag
from tdp.core.models import Base, keyvalgen
from tdp.core.models.action_log import ActionLog
from tdp.core.models.deployment_log import DeploymentLog
from tdp.core.runner.action_runner import ActionRunner
from tdp.core.runner.ansible_executor import AnsibleExecutor

GREEN = "\033[0;32m"
END = "\033[0m"

DAG = Dag()

logger = logging.getLogger("devtools.tdp")


# ----------- Logging configuration


class CommandAdapter(logging.LoggerAdapter):
    def processs(self, msg, kwargs):
        return "[%s] %s" % (self.extra["command"], msg), kwargs


class CommandFilter(logging.Filter):
    def filter(self, record):
        if not "command" in record.__dict__:
            setattr(record, "command", "MAIN")
        return True


logger_handler = logging.StreamHandler()
logger_handler.setFormatter(
    logging.Formatter(
        f"[{GREEN}%(command)s{END}] [%(levelname)s] %(funcName)s - %(message)s"
    )
)

logger.setLevel(logging.DEBUG)
logger.addHandler(logger_handler)
logger.addFilter(CommandFilter())

adapter = CommandAdapter(logger, {"command": "MAIN"})


# ----------- Argument definition


def arguments_definition():
    parser = argparse.ArgumentParser(description="TDP Runner")
    parser.set_defaults(command="main")
    subparsers = parser.add_subparsers()
    nodes_parser = subparsers.add_parser("nodes", help="List nodes from components DAG")
    nodes_parser.set_defaults(command="nodes")
    deploy_parser = subparsers.add_parser("deploy", help="Deploy's help")
    deploy_parser.set_defaults(command="deploy")
    fill_deploy_parser(deploy_parser)
    browse_parser = subparsers.add_parser("browse", help="Browse's help")
    browse_parser.set_defaults(command="browse")
    fill_browse_parser(browse_parser)
    return parser


def fill_deploy_parser(parser):
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
        "--playbooks_directory",
        action=EnvDefault,
        envvar="TDP_PLAYBOOKS_DIRECTORY",
        type=Path,
        default=None,
        help="Path to tdp-collection playbooks",
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
        "--filter",
        type=str,
        default=None,
        help="Glob on list name",
    )
    parser.add_argument(
        "--dry", action="store_true", help="Execute dag without running any action"
    )


def fill_browse_parser(parser):
    parser.add_argument(
        "deployment_id", nargs="?", type=int, default=None, help="Deployment to display"
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


def format_deployment_log(deployment_log, headers):
    def custom_format(key, value):
        if key == "actions":
            if len(value) > 2:
                return [value[0].action, "...", value[-1].action]
            else:
                return [action.action for action in value]
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


# ----------- Process query functions


def process_deployments_query(adapter, session_class):
    headers = [key for key, _ in keyvalgen(DeploymentLog)]
    query = select(DeploymentLog).order_by(DeploymentLog.id)

    with session_class() as session:
        result = session.execute(query).scalars().fetchall()
        adapter.info(
            "Deployments:\n"
            + tabulate(
                [
                    format_deployment_log(deployment_log, headers)
                    for deployment_log in result
                ],
                headers="keys",
            )
        )


def process_single_deployment_query(adapter, session_class, deployment_id):
    deployment_headers = [key for key, _ in keyvalgen(DeploymentLog)]
    action_headers = [key for key, _ in keyvalgen(ActionLog)]
    action_headers.remove("deployment")
    query = (
        select(DeploymentLog)
        .where(DeploymentLog.id == deployment_id)
        .order_by(DeploymentLog.id)
    )

    with session_class() as session:
        result = session.execute(query).scalars().fetchall()
        adapter.info(
            "Deployment:\n"
            + tabulate(
                [
                    format_deployment_log(deployment_log, deployment_headers)
                    for deployment_log in result
                ],
                headers="keys",
            )
        )
        adapter.info(
            "Actions:\n"
            + tabulate(
                [
                    format_action_log(action_log, action_headers)
                    for action_log in result[0].actions
                ],
                headers="keys",
            )
        )


def process_action_query(adapter, session_class, deployment_id, action):
    headers = [key for key, _ in keyvalgen(ActionLog)]
    headers.remove("deployment")
    query = (
        select(ActionLog)
        .where(ActionLog.deployment_id == deployment_id)
        .where(ActionLog.action == action)
        .order_by(ActionLog.start)
    )
    with session_class() as session:
        result = session.execute(query).scalars().fetchall()
        action_logs = [action_log for action_log in result]
        adapter.info(
            "Action:\n"
            + tabulate(
                [format_action_log(action_log, headers) for action_log in action_logs],
                headers="keys",
            )
        )
        if action_logs:
            action_log = action_logs[0]
            adapter.info(f"{action_log.action} logs:\n" + str(action_log.logs, "utf-8"))


# ----------- Commands


def list_nodes(adapter):
    endline = "\n- "
    components = endline.join(component for component in DAG.get_all_actions())
    adapter.info(f"Component list:{endline}{components}")


def deploy_action(
    adapter,
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
        adapter.info(f"Deploying {target}")
        deployment = action_runner.run_to_node(target, filter)
        session.add(deployment)
        session.commit()


def browse_action(adapter, deployment_id=None, action=None, sqlite_path=None):
    if sqlite_path is None:
        raise ValueError("SQLITE_PATH cannot be None")
    session_class = get_session_class(sqlite_path)

    if not deployment_id:
        process_deployments_query(adapter, session_class)
    else:
        if not action:
            process_single_deployment_query(adapter, session_class, deployment_id)
        else:
            process_action_query(adapter, session_class, deployment_id, action)


# ----------- Main


def main():
    args = arguments_definition().parse_args()
    logger.debug("Arguments: " + str(vars(args)))
    adapter = CommandAdapter(logger, {"command": args.command.upper()})
    if args.command == "nodes":
        list_nodes(adapter)
    elif args.command == "deploy":
        deploy_action(
            adapter,
            playbooks_directory=args.playbooks_directory,
            target=args.target,
            run_directory=args.run_directory,
            filter=args.filter,
            sqlite_path=args.sqlite_path,
            dry=args.dry,
        )
    elif args.command == "browse":
        browse_action(
            adapter,
            deployment_id=args.deployment_id,
            action=args.action,
            sqlite_path=args.sqlite_path,
        )

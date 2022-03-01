import argparse
import logging


from tdp.core.dag import Dag
from tdp.devtools.run.colors import END, GREEN
from tdp.devtools.run.commands.browse import BrowseCommand
from tdp.devtools.run.commands.deploy import DeployCommand
from tdp.devtools.run.commands.default_diff import DefaultDiffCommand
from tdp.devtools.run.commands.init import InitCommand
from tdp.devtools.run.commands.nodes import NodesCommand
from tdp.devtools.run.commands.service_versions import ServicesVersionCommand

DAG = Dag()

logger = logging.getLogger("devtools.tdp")


# ----------- Logging configuration


class CommandAdapter(logging.LoggerAdapter):
    def processs(self, msg, kwargs):
        if self.extra:
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


# ----------- Argument definition


def arguments_definition(command_classes):
    parser = argparse.ArgumentParser(description="TDP Runner")
    parser.set_defaults(command=main_command)
    subparsers = parser.add_subparsers()
    for command_class in command_classes:
        sub_parser = subparsers.add_parser(
            command_class.command(), help=command_class.__doc__
        )
        command_class.fill_argument_definition(sub_parser, dag=DAG)
    return parser


def main_command(adapter, args, dag=None):
    adapter.error("You need to specify a command")
    args.parser.print_help()


def main():
    parser = arguments_definition(
        [
            NodesCommand,
            BrowseCommand,
            DeployCommand,
            InitCommand,
            DefaultDiffCommand,
            ServicesVersionCommand,
        ]
    )
    args = parser.parse_args()
    logger.debug(f"args: {vars(args)}")
    if args.command == main_command:
        adapter = CommandAdapter(logger, {"command": "MAIN"})
        args.command(adapter, argparse.Namespace(parser=parser), dag=DAG)
    else:
        adapter = CommandAdapter(logger, {"command": args.command.command().upper()})
        args.command(adapter, args, DAG).run()

from tdp.devtools.run.commands.command import Command


class NodesCommand(Command):
    """List nodes from components DAG"""

    def __init__(self, adapter, args, dag) -> None:
        super().__init__(adapter, args, dag)

    def run(self):
        endline = "\n- "
        components = endline.join(component for component in self.dag.get_all_actions())
        self.adapter.info(f"Component list:{endline}{components}")

    @staticmethod
    def fill_argument_definition(parser, dag):
        parser.set_defaults(command=NodesCommand)

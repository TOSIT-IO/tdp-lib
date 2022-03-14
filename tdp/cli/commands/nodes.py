import click

from tdp.cli.context import pass_dag


@click.command(short_help="List nodes from components DAG")
@pass_dag
def nodes(dag):
    endline = "\n- "
    components = endline.join(component for component in dag.get_all_actions())
    click.echo(f"Component list:{endline}{components}")

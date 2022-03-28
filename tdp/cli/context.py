import click

from tdp.core.dag import Dag

pass_dag = click.make_pass_decorator(Dag)

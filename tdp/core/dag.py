from copy import Error
from networkx.classes.function import subgraph
import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


from pathlib import Path
import networkx as nx


class Dag:
    """Generate DAG with components dependencies"""

    def __init__(self, yaml_files=None):
        self._components = None
        self._graph = None

        if yaml_files is None:
            yaml_files = [Path(__file__).with_name("components.yml")]
        self.yaml_files = yaml_files

    @property
    def components(self):
        if self._components is not None:
            return self._components

        components_list = []
        for yaml_file in self.yaml_files:
            with yaml_file.open("r") as component_file:
                components_list.extend(yaml.load(component_file, Loader=Loader) or [])

        components = {}
        for component in components_list:
            name = component['name']
            depends_on = component['depends_on']
            if name in components:
                raise ValueError(f'"{name}" is declared several times')
            components[name] = depends_on

        self._components = components
        return self._components

    @property
    def graph(self):
        if self._graph is not None:
            return self._graph

        component_names = sorted(self.components.keys())
        DG = nx.DiGraph()
        DG.add_nodes_from(component_names)

        for component_name in component_names:
            for dependency in sorted(self.components[component_name]):
                if dependency not in self.components:
                    raise ValueError(f'Dependency "{dependency}" does not exist for component "{component_name}"')
                DG.add_edge(dependency, component_name)

        if nx.is_directed_acyclic_graph(DG):
            self._graph = DG
            return self._graph
        else:
            raise ValueError('Not a DAG')

    def get_action_to_node(self, node):
        actions = list(nx.lexicographical_topological_sort(self.graph.subgraph(nx.ancestors(self.graph, node))))
        actions.append(node)
        return actions


if __name__ == "__main__":
    dag = Dag()
    print(dag.get_action_to_node('hdfs_init'))

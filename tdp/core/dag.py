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

    def __init__(self):
        self._components = None
        self._graph = None

    @property
    def components(self):
        if self._components is not None:
            return self._components

        # TODO load from multiple files
        component_file_path = Path(__file__).with_name("components.yml")
        with component_file_path.open("r") as component_file:
            self._components = yaml.load(component_file, Loader=Loader) or {}

            # TODO sort content
            return self._components

    @property
    def graph(self):
        if self._graph is not None:
            return self._graph

        DG = nx.DiGraph()
        for component in self.components:
            DG.add_node(component['name'])

        for component in self.components:
            for dependency in component['depends_on']:
                for checkcomp in self.components:
                    if dependency == checkcomp['name']:
                        DG.add_edge(dependency, component['name'])
                        break
                else:
                    raise ValueError('Depedency ' + dependency + ' does not exist')

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

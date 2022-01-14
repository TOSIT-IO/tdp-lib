from copy import Error
from networkx.classes.function import subgraph
import yaml
import logging

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import tdp.components
from tdp.core.component import Component
from tdp.core.runner import Runner


from pathlib import Path
import fnmatch
import re
import networkx as nx

logger = logging.getLogger("tdp").getChild("dag")

class Dag:
    """Generate DAG with components dependencies"""

    def __init__(self, yaml_files=None):
        self._components = None
        self._graph = None
        self._yaml_files = None

        if yaml_files is None:
            yaml_files = list((Path(tdp.components.__file__).parent).glob("*.yml"))
        self.yaml_files = yaml_files

    @property
    def yaml_files(self):
        if self._yaml_files is not None:
            return self._yaml_files
        return []

    @yaml_files.setter
    def yaml_files(self, value):
        self._yaml_files = value
        del self.components

    @yaml_files.deleter
    def yaml_files(self):
        self.yaml_files = None

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
            if name in components:
                raise ValueError(f'"{name}" is declared several times')
            components[name] = Component(**component)

        self._components = components
        return self._components

    @components.setter
    def components(self, value):
        self._components = value
        del self.graph

    @components.deleter
    def components(self):
        self.components = None

    @property
    def graph(self):
        if self._graph is not None:
            return self._graph

        component_names = sorted(self.components.keys())
        DG = nx.DiGraph()
        DG.add_nodes_from(component_names)

        for component_name in component_names:
            component = self.components[component_name]
            for dependency in sorted(component.depends_on):
                if dependency not in self.components:
                    raise ValueError(f'Dependency "{dependency}" does not exist for component "{component_name}"')
                DG.add_edge(dependency, component_name)

        if nx.is_directed_acyclic_graph(DG):
            self._graph = DG
            return self._graph
        else:
            raise ValueError('Not a DAG')

    @graph.setter
    def graph(self, value):
        self._graph = value

    @graph.deleter
    def graph(self):
        self.graph = None

    def get_actions_to_node(self, node):
        actions = list(nx.lexicographical_topological_sort(self.graph.subgraph(nx.ancestors(self.graph, node))))
        actions.append(node)
        return actions

    def filter_actions_glob(self, actions, glob):
        return fnmatch.filter(actions, glob)

    def filter_actions_regex(self, actions, regex):
        compiled_regex = re.compile(regex)
        return list(filter(compiled_regex.match, actions))


if __name__ == "__main__":
    dag = Dag()
    print(dag.get_actions_to_node('hdfs_init'))


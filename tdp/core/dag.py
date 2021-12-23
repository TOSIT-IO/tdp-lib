from copy import Error
from networkx.classes.function import subgraph
import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from tdp.core.runner import Runner

from pathlib import Path
import fnmatch
import re
import networkx as nx


class Dag:
    """Generate DAG with components dependencies"""

    def __init__(self, yaml_files=None):
        self._components = None
        self._graph = None
        self._yaml_files = None

        self._failed_nodes = []
        self._success_nodes = []
        self._skipped_nodes = []

        if yaml_files is None:
            yaml_files = [Path(__file__).with_name("components.yml")]
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
            depends_on = component['depends_on']
            if name in components:
                raise ValueError(f'"{name}" is declared several times')
            components[name] = depends_on

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
            for dependency in sorted(self.components[component_name]):
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
        return [x for x in map(lambda action: action if re.match(regex, action) else None, actions) if x is not None]

    def run_to_node(self, node, runner, filter_glob=None, filter_regex=None):
        actions = self.get_actions_to_node(node)
        if filter_glob:
            actions = self.filter_actions_glob(actions, filter_glob)
        if filter_regex:
            actions = self.filter_actions_regex(actions, filter_regex)

        for action in actions:
            if action not in self._failed_nodes + self._skipped_nodes:
                res = runner.run(action)
                if res['is_failed']:
                    print(f'Action {action} failed !')  
                    self._failed_nodes.append(action)
                    for desc in nx.descendants(self.graph, action):
                        print(f'Action {desc} will be skipped')
                        self._skipped_nodes.append(desc)

                    print('Resuming')
                else:
                    print(f'Action {action} success')
                    self._success_nodes.append(action)



if __name__ == "__main__":
    dag = Dag()
    print(dag.get_actions_to_node('hdfs_init'))


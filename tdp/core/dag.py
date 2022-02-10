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

from pathlib import Path
import fnmatch
import re
import networkx as nx
import os

logger = logging.getLogger("tdp").getChild("dag")


class Dag:
    """Generate DAG with components dependencies"""

    def __init__(self, yaml_files=None, playbooks_dir=None):
        self._components = None
        self._graph = None
        self._yaml_files = None
        self.playbooks_dir = playbooks_dir

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
            name = component["name"]
            if name in components:
                raise ValueError(f'"{name}" is declared several times')
            components[name] = Component(**component)

        self._components = components
        self.validate()
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
                    raise ValueError(
                        f'Dependency "{dependency}" does not exist for component "{component_name}"'
                    )
                DG.add_edge(dependency, component_name)

        if nx.is_directed_acyclic_graph(DG):
            self._graph = DG
            return self._graph
        else:
            raise ValueError("Not a DAG")

    @graph.setter
    def graph(self, value):
        self._graph = value

    @graph.deleter
    def graph(self):
        self.graph = None

    def get_actions(self, node=None):
        if node:
            return self.get_actions_to_node(node)
        return self.get_all_actions()

    def get_actions_to_node(self, node):
        actions = list(
            nx.lexicographical_topological_sort(
                self.graph.subgraph(nx.ancestors(self.graph, node))
            )
        )
        actions.append(node)
        return actions

    def get_all_actions(self):
        return list(nx.lexicographical_topological_sort(self.graph))

    def filter_actions_glob(self, actions, glob):
        return fnmatch.filter(actions, glob)

    def filter_actions_regex(self, actions, regex):
        compiled_regex = re.compile(regex)
        return list(filter(compiled_regex.match, actions))

    def validate(self):
        """
        Validation rules :
        - *_start actions can only be required from within its own service
        - *_install actions should only depend on other *_install actions
        - Each service (HDFS, HBase, Hive, etc) should have *_install, *_config, *_init and *_start actions
          even if they are "empty" (tagged with noop)
        - Actions tagged with the noop flag should not have a playbook defined in the collection
        """
        # key: service_name
        # value: set of available actions for the service
        services_actions = {}

        if not self.playbooks_dir:
            logger.warning(f"playbooks_dir is not defined, skip playbooks validations")

        for component_name, component in self.components.items():
            for dependency in component.depends_on:
                # *_start actions can only be required from within its own service
                dependency_service = self.components[dependency].service
                if (
                    dependency.endswith("_start")
                    and dependency_service != component.service
                ):
                    logger.warning(
                        f"Component '{component_name}' is in service '{component.service}', depends on "
                        f"'{dependency}' which is a start action in service '{dependency_service}' and should "
                        f"only depends on start action within its own service"
                    )

                # *_install actions should only depend on other *_install actions
                if component_name.endswith("_install") and not dependency.endswith(
                    "_install"
                ):
                    logger.warning(
                        f"Component '{component_name}' is an install action, depends on '{dependency}' which is "
                        f"not an install action and should only depends on other install action"
                    )

            # Each service (HDFS, HBase, Hive, etc) should have *_install, *_config, *_init and *_start actions
            # even if they are "empty" (tagged with noop)
            # Part 1
            if component.is_service():
                services_actions.setdefault(component.service, set()).add(
                    component.action
                )

            # Actions tagged with the noop flag should not have a playbook defined in the collection
            if self.playbooks_dir:
                playbooks = os.listdir(self.playbooks_dir)
                if f"{component_name}.yml" in playbooks:
                    if component.noop:
                        logger.warning(
                            f"Component '{component_name}' is noop and the playbook should not exist"
                        )
                else:
                    if not component.noop:
                        logger.warning(
                            f"Component '{component_name}' should have a playbook"
                        )

        # Each service (HDFS, HBase, Hive, etc) should have *_install, *_config, *_init and *_start actions
        # even if they are "empty" (tagged with noop)
        # Part 2
        actions_for_service = {"install", "config", "start", "init"}
        for service, actions in services_actions.items():
            if not actions.issuperset(actions_for_service):
                logger.warning(
                    f"Service '{service}' have these actions {actions} and at least one action is missing from "
                    f"{actions_for_service}"
                )


if __name__ == "__main__":
    dag = Dag()
    print(dag.get_actions_to_node("hdfs_init"))

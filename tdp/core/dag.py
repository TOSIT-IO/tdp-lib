from copy import Error
from networkx.classes.function import subgraph
import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


from pathlib import Path
import networkx as nx


def load_components():
    # TODO load from multiple files
    component_file_path = Path(__file__).with_name("components.yml")
    with component_file_path.open("r") as component_file:
        content = yaml.load(component_file, Loader=Loader) or {}
         
        # TODO sort content
        return content


def load_dag(content):
    DG = nx.DiGraph()
    for component in content:
        DG.add_node(component['name'])

    for component in content:
        for dependency in component['depends_on']:
            for checkcomp in content:
                if dependency == checkcomp['name']:
                    DG.add_edge(dependency, component['name'])
                    break
            else:
                raise ValueError('Depedency ' + dependency + ' does not exist')

    if nx.is_directed_acyclic_graph(DG):
        return DG
    else:
        raise ValueError('Not a DAG')


def get_actions_to_node(dag, node):
    actions = list(nx.lexicographical_topological_sort(dag.subgraph(nx.ancestors(dag, node))))
    actions.append(node)
    return actions



if __name__ == "__main__":
    components = load_components()
    dag = load_dag(components)

    print(get_actions_to_node(dag, 'hdfs_init'))

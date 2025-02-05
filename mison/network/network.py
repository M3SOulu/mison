from typing import Union, Callable, TypeAlias

from mison.miner import Commit

from collections.abc import Mapping

import networkx as nx

__all__ = ['DevComponentMapping', 'DevFileMapping', 'get_dev_file_mapping', 'quick_clean_devs', 'split_bipartite_nodes',
           'map_developers', 'map_files_to_components']

DevFileMapping: TypeAlias = nx.Graph
DevComponentMapping: TypeAlias = nx.Graph


def quick_clean_devs(G: Union[DevComponentMapping, DevFileMapping]):
    stop_list = {"(none)"}
    nodes_remove = {node for node, data in G.nodes(data=True) if data["type"] == "dev" and node in stop_list}
    for node in nodes_remove:
        print(f"Found {node}; to be removed")
    G.remove_nodes_from(nodes_remove)
    return G

def split_bipartite_nodes(G: Union[DevFileMapping, DevComponentMapping], type):
    top = {n for n, d in G.nodes(data=True) if d["type"] == type}
    bottom = set(G) - top
    return top, bottom


def get_dev_file_mapping(commit_table, repository=None):
    G: DevFileMapping = nx.Graph()
    for row in commit_table.itertuples(index=False):
        dev = row.author_email
        file = row.filename
        commit = Commit(sha=row.commit_hash, author_name=row.author_name, author_email=row.author_email,
                        committer_name=row.committer_name, committer_email=row.committer_email,
                        commit_date=row.commit_date, filename=file, additions=row.additions, deletions=row.deletions)
        G.add_node(dev, type='dev')
        G.add_node(file, type='file', repository=repository)
        if G.has_edge(dev, file):
            G[dev][file]['commits'] += [commit]
        else:
            G.add_edge(dev, file, commits=[commit])
    return G


def map_developers(G: Union[DevFileMapping, DevComponentMapping], developer_mapping: Union[Mapping, Callable]):
    devs, _ = split_bipartite_nodes(G, 'dev')
    if callable(developer_mapping):
        mapping_iter = map(developer_mapping, devs)
    elif isinstance(developer_mapping, Mapping):
        mapping_iter = map(lambda x: developer_mapping.get(x, x), devs)
    else:
        raise ValueError("developer_mapping must be a Mapping or a Callable")
    for old_dev, new_dev in zip(devs, mapping_iter):
        if old_dev == new_dev:
            print(f"Keeping {old_dev}")
            continue
        print(f"Replacing {old_dev} with {new_dev}")
        for _, file, data in G.edges(old_dev, data=True):
            G.add_edge(new_dev, file, **data)
        G.remove_node(old_dev)
    return G


def map_files_to_components(G: DevFileMapping, component_mapping: Union[Mapping, Callable]):
    devs, files = split_bipartite_nodes(G, 'dev')
    D: DevComponentMapping = nx.Graph()
    D.add_nodes_from(devs, type='dev')
    if callable(component_mapping):
        mapping_iter = map(component_mapping, files)
    elif isinstance(component_mapping, Mapping):
        mapping_iter = map(lambda x: component_mapping.get(x, None), files)
    else:
        raise ValueError("component_mapping must be a Mapping or a Callable")
    for file, component in zip(files, mapping_iter):
        if component is None:
            print(f"File {file} does not belong to a component")
            continue
        print(f"File {file} belongs to {component}")
        D.add_node(component, type='component')
        for _, dev, data in G.edges(file, data=True):
            D.add_edge(dev, component, **data)
    return D

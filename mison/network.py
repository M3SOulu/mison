import itertools
from typing import Union, Callable

from .mine import Commit

from collections.abc import Mapping

import networkx as nx
from networkx.algorithms import bipartite
import numpy as np

__all__ = ['construct_bipartite', 'developer_collaboration_network_count', 'developer_collaboration_network_cosine',
           'quick_clean_devs', 'split_bipartite_nodes', 'map_developers', 'map_files_to_components']


def quick_clean_devs(G: nx.Graph):
    stop_list = {"(none)"}
    nodes_remove = {node for node, data in G.nodes(data=True) if data["bipartite"] == "dev" and node in stop_list}
    for node in nodes_remove:
        print(f"Found {node}; to be removed")
    G.remove_nodes_from(nodes_remove)
    return G

def split_bipartite_nodes(G, bipartite):
    top = {n for n, d in G.nodes(data=True) if d["bipartite"] == bipartite}
    bottom = set(G) - top
    return top, bottom


def construct_bipartite(commit_table, repository=None):
    G = nx.Graph()
    for row in commit_table.itertuples(index=False):
        dev = row.author_email
        file = row.filename
        commit = Commit(sha=row.commit_hash, author_name=row.author_name, author_email=row.author_email,
                        committer_name=row.committer_name, committer_email=row.committer_email,
                        commit_date=row.commit_date, filename=file, additions=row.additions, deletions=row.deletions)
        G.add_node(dev, bipartite='dev')
        G.add_node(file, bipartite='file', repository=repository)
        if G.has_edge(dev, file):
            G[dev][file]['commits'] += [commit]
        else:
            G.add_edge(dev, file, commits=[commit])
    return G

def map_developers(G: nx.Graph, developer_mapping: Union[Mapping, Callable]):
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


def map_files_to_components(G: nx.Graph, component_mapping: Union[Mapping, Callable]):
    devs, files = split_bipartite_nodes(G, 'dev')
    D = nx.Graph()
    D.add_nodes_from(devs, bipartite='dev')
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
        D.add_node(component, bipartite='component')
        for _, dev, data in G.edges(file, data=True):
            D.add_edge(dev, component, **data)
    return D


def developer_collaboration_network_count(G):
    devs, _ = split_bipartite_nodes(G, 'dev')
    D = bipartite.weighted_projected_graph(G, nodes=devs, ratio=False)
    return D


def developer_collaboration_network_cosine(G: nx.Graph):
    devs, files = split_bipartite_nodes(G, "dev")
    devs = sorted(devs)
    files = sorted(files)
    N_devs = len(devs)
    N_files = len(files)
    weight = np.zeros(shape=(N_devs, N_files))
    indexed_devs = {dev: i for i, dev in enumerate(devs)}
    indexed_files = {file: i for i, file in enumerate(files)}
    log_degree = {file: np.log(N_files / G.degree[file]) for file in files}

    for file, dev in itertools.product(files, devs):
        if G.has_edge(file, dev):
            file_index = indexed_files[file]
            dev_index = indexed_devs[dev]
            weight[dev_index, file_index] = len(G[file][dev]["commits"]) * log_degree[file]

    # Compute similarity using NumPy operations
    norms = np.linalg.norm(weight, axis=1, keepdims=True)
    normalized_weights = weight / (norms + 1e-10)  # Avoid division by zero
    similarity_matrix = np.dot(normalized_weights, normalized_weights.T)

    D = bipartite.generic_weighted_projected_graph(G, devs,lambda G, u, v: float(similarity_matrix[indexed_devs[u], indexed_devs[v]]))
    return D

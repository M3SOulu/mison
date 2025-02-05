import itertools
from typing import Union, TypeAlias

import networkx as nx
import numpy as np
from networkx.algorithms import bipartite

from mison.network import split_bipartite_nodes, DevComponentMapping, DevFileMapping

__all__ = ["count_network", "cosine_network", "DevCollaboration"]

DevCollaboration: TypeAlias = nx.Graph


def count_network(G: Union[DevComponentMapping, DevFileMapping]):
    devs, _ = split_bipartite_nodes(G, 'dev')
    D: DevCollaboration = bipartite.weighted_projected_graph(G, nodes=devs, ratio=False)
    return D


def cosine_network(G: Union[DevComponentMapping, DevFileMapping]):
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

    D: DevCollaboration = bipartite.generic_weighted_projected_graph(G, devs,lambda G, u, v: float(similarity_matrix[indexed_devs[u], indexed_devs[v]]))
    return D

import itertools
from typing import Union, TypeAlias

import networkx as nx
import numpy as np
from networkx.algorithms import bipartite

from mison.network import split_bipartite_nodes, DevComponentMapping, DevFileMapping

__all__ = ["count_network", "cosine_network", "DevCollaboration"]

DevCollaboration: TypeAlias = nx.Graph


def count_network(G: Union[DevComponentMapping, DevFileMapping]):
    """
    Construct a `DevCollaboration` network from a `DevFileMapping` or `DevComponentMapping` graph.

    This function builds a `DevCollaboration` network by analyzing how many files or components
    a pair of developers have both modified. The number of shared files/components determines the
    edge weight between developers, following the approach described in [1].

    ### Graph Structure:
    - **Nodes**: Developers.
    - **Edges**: A weighted edge exists between two developers if they have both modified at least one
      common file (in `DevFileMapping`) or component (in `DevComponentMapping`).
    - **Edge Weights**: The weight of an edge represents the number of shared files or components
      modified by both developers.

    ### Reference:
    [1] Li, X., Abdelfattah, A. S., Yero, J., d'Aragona, D. A., Cerny, T., & Taibi, D. (2023, July).
        "Analyzing organizational structure of microservice projects based on contributor collaboration."
        *2023 IEEE International Conference on Service-Oriented System Engineering (SOSE)*, pp. 1-8, IEEE.

    :param G: A `DevFileMapping` or `DevComponentMapping` graph.
    :return: A `DevCollaboration` graph where nodes represent developers, and edge weights correspond
             to the number of shared files or components.
    """
    devs, _ = split_bipartite_nodes(G, 'dev')
    D: DevCollaboration = bipartite.weighted_projected_graph(G, nodes=devs, ratio=False)
    return D


def cosine_network(G: Union[DevComponentMapping, DevFileMapping]):
    """
    Construct a `DevCollaboration` network using cosine similarity of log-normalized developer activity vectors.

    This function generates a `DevCollaboration` graph from a `DevFileMapping` or `DevComponentMapping`
    graph by computing the similarity between developers based on the files or components they have modified.
    The weight of an edge between two developers is given by the **cosine similarity** of their activity vectors,
    where each vector entry represents the **log-normalized** count of commits to a particular file or component,
    following the approach described in [1].

    ### Graph Structure:
    - **Nodes**: Developers.
    - **Edges**: A weighted edge exists between two developers if their activity vectors (i.e., the files
      or components they have modified) have a nonzero cosine similarity.
    - **Edge Weights**: The weight of an edge between developers `u` and `v` is computed as:

      \[
      w(u, v) = \frac{\mathbf{w}_u \cdot \mathbf{w}_v}{\|\mathbf{w}_u\| \|\mathbf{w}_v\|}
      \]

      where:
      - \(\mathbf{w}_u\) and \(\mathbf{w}_v\) are the **log-normalized** activity vectors of developers `u` and `v`.
      - Each entry in the vector represents the **log-transformed commit count** of a developer to a specific file or component:

        \[
        w_{u, f} = \text{commits}(u, f) \times \log\left(\frac{N_{\text{files}}}{\text{degree}(f)}\right)
        \]

        where:
        - \(\text{commits}(u, f)\) is the number of commits made by developer `u` to file `f`.
        - \(N_{\text{files}}\) is the total number of files in the dataset.
        - \(\text{degree}(f)\) is the number of developers who have modified file `f`.

      - Higher similarity values indicate stronger collaboration between developers.

    ### Reference:
    [1] Jermakovics, A., Sillitti, A., & Succi, G. (2011, May). "Mining and visualizing developer networks
        from version control systems." *Proceedings of the 4th International Workshop on Cooperative and
        Human Aspects of Software Engineering*, pp. 24-31.

    :param G: A `DevFileMapping` or `DevComponentMapping` graph.
    :return: A `DevCollaboration` graph where nodes represent developers, and edge weights correspond
             to the cosine similarity of their log-normalized activity vectors.
    """
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

from typing import List, Set, TypeAlias
from collections import Counter, defaultdict
from statistics import harmonic_mean
from itertools import pairwise

import networkx as nx
from networkx import bipartite

from mison.network import DevComponentMapping
from mison.miner import Commit

__all__ = ['organizational_coupling', 'logical_coupling']

ComponentCoupling: TypeAlias = nx.Graph

def organizational_coupling(G: DevComponentMapping) -> ComponentCoupling:
    """
    Calculate the organizational coupling of components from a `DevComponentMapping` graph.

    This function measures the **organizational coupling** of software components by analyzing
    developer contributions, following the approach described in [1]. The coupling is based on
    two key factors:

    1. **Per-Component Contributions**:
       - For each developer and component, the function quantifies the number of modifications
         the developer has made to that component (microservice).

    2. **Cross-Component Contribution Switching**:
       - For each developer and a **pair of components**, the function counts how often the
         developer has switched contributions between the two components.
       - Higher switching frequency between two components indicates a **stronger organizational coupling**.

    ### Graph Structure:
    - **Nodes**: Components (microservices).
    - **Edges**: Weighted edges represent the **organizational coupling strength** between two components
      based on shared developer contributions.
    - **Edge Weights**: The weight of an edge is determined by the number of contributions and
      contribution switches to given components.

    ### Reference:
    [1] Li, Xiaozhou, Dario Amoroso d’Aragona, and Davide Taibi.
        "Evaluating Microservice Organizational Coupling Based on Cross-Service Contribution."
        *International Conference on Product-Focused Software Process Improvement.*
        Cham: Springer Nature Switzerland, 2023.

    :param G: A `DevComponentMapping` graph representing developer contributions to components.
    :return: A `ComponentCoupling` graph where nodes represent components, and edge weights indicate
             the level of organizational coupling between them.
    """
    devs = set()
    components = set()
    contribution_switch = defaultdict(float)  # Contributions switches between two components done by dev
    contribution_value = Counter()  # Contribution values for a components by devs
    dev_commits_to_ms = defaultdict(set)
    commits_to_ms_mapping = defaultdict(set)  # Mapping of a commit SHA to the components it touched
    for dev, d in G.nodes(data=True):
        if d["type"] != 'dev':
            components.add(dev)
            continue
        devs.add(dev)
        dev_commits_set: set[Commit] = set()
        for _, component, data in G.edges(dev, data=True):
            for commit in data["commits"]:
                # Developer made this commit
                dev_commits_set.add(commit)
                # Developer made this commit to a specific service
                dev_commits_to_ms[dev, component].add(commit.sha)
                # Map commit SHA to component it touched
                commits_to_ms_mapping[commit.sha].add(component)
                # Calculate contribution value of dev to component by summing over all files of component
                component_files = G.nodes[component]["files"]
                contribution_value[(dev, component)] += sum((mod_file.additions + mod_file.deletions)
                                                            for mod_file in commit.modified_files
                                                            if mod_file.path in component_files)

        # Get the list of commits a dev made sequentially
        dev_commits_list: List[Commit] = sorted(dev_commits_set, key=lambda x: x.commit_date)
        # Get the list of components a dev touched with their commits sequentially
        dev_component_list: List[Set[str]] = [commits_to_ms_mapping.get(x.sha) for x in dev_commits_list]
        # Calculate contribution switches
        for prev_commit, next_commit in pairwise(dev_component_list):
            for new_ms in next_commit:
                for old_ms in prev_commit:
                    if new_ms != old_ms:
                        n = len(dev_commits_to_ms[(dev, new_ms)] | dev_commits_to_ms[(dev, old_ms)])
                        contribution_weight = 1/(2*(n-1))
                        contribution_switch[frozenset([old_ms, new_ms, dev])] += contribution_weight

    def org_coupling(G, u, v):
        weight = 0.0
        for dev in devs:
            weight += contribution_switch.get(frozenset([u, v, dev]), 0.0) * harmonic_mean([contribution_value[(dev, u)], contribution_value[(dev, v)]])
        return weight

    D = bipartite.generic_weighted_projected_graph(G, components, org_coupling)
    return D


def logical_coupling(G: DevComponentMapping):

    component_commits = defaultdict(set)
    components = set()
    for component in G:
        if G.nodes[component]["type"] != 'component':
            continue
        components.add(component)
        for _, _, data in G.edges(component, data=True):
            component_commits[component].update(data["commits"])

    D = bipartite.generic_weighted_projected_graph(G, components, lambda G, u, v: len(component_commits[u] & component_commits[v]))
    return D
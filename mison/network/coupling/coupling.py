from typing import List, Set, TypeAlias
from collections import Counter, defaultdict
from statistics import harmonic_mean
from itertools import pairwise

import networkx as nx
from networkx import bipartite

from mison.network import split_bipartite_nodes, DevComponentMapping
from mison.miner import Commit

__all__ = ['organizational_coupling', 'logical_coupling']

ComponentCoupling: TypeAlias = nx.Graph

def organizational_coupling(G: DevComponentMapping):
    devs, components = split_bipartite_nodes(G, 'dev')
    contribution_switch = defaultdict(float)  # Contributions switches between two components done by dev
    contribution_value = Counter()  # Contribution values for a components by devs
    dev_commits_to_ms = defaultdict(set)
    commits_to_ms_mapping = defaultdict(set)  # Mapping of a commit SHA to the components it touched
    for dev in devs:
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

    components, _ = split_bipartite_nodes(G, 'component')
    component_commits = defaultdict(set)
    for component in components:
        for _, _, data in G.edges(component, data=True):
            component_commits[component].update(data["commits"])

    D = bipartite.generic_weighted_projected_graph(G, components, lambda G, u, v: len(component_commits[u] & component_commits[v]))
    return D
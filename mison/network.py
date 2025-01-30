from .mine import Commit

import networkx as nx
from networkx.algorithms import bipartite

__all__ = ['construct_bipartite', 'developer_collaboration_network']


def construct_bipartite(commit_table):
    G = nx.Graph()
    for row in commit_table.itertuples(index=False):
        dev = row.author_email
        file = row.filename
        commit = Commit(sha=row.commit_hash, author_name=row.author_name, author_email=row.author_email,
                        committer_name=row.committer_name, committer_email=row.committer_email,
                        commit_date=row.commit_date, filename=file, additions=row.additions, deletions=row.deletions)
        G.add_node(dev, bipartite='dev')
        G.add_node(file, bipartite='file')
        if G.has_edge(dev, file):
            G[dev][file]['commits'] += [commit]
        else:
            G.add_edge(dev, file, commits=[commit])
    return G

def developer_collaboration_network(G):
    devs = {n for n, d in G.nodes(data=True) if d["bipartite"] == 'dev'}
    D = bipartite.weighted_projected_graph(G, nodes=devs, ratio=False)
    return D

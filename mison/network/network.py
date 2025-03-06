from mison.miner import Commit

from collections.abc import Mapping
from typing import Union, Callable, List

import networkx as nx
from pydriller import ModificationType

__all__ = ['DevComponentMapping', 'DevFileMapping', 'split_bipartite_nodes', 'quick_clean_devs', 'DEV_STOP_LIST']

DEV_STOP_LIST = {"(none)", ""}

def quick_clean_devs(G):
    """
    Remove developers who found in a common stoplist.

    :param G: A graph of either DevComponentMapping or DevFileMapping
    :return: The filtered graph (graph is modified in-place)
    """
    nodes_remove = {node for node, data in G.nodes(data=True) if data["type"] == "dev" and node in DEV_STOP_LIST}
    for node in nodes_remove:
        print(f"Found {node}; to be removed")
    G.remove_nodes_from(nodes_remove)


class DevFileMapping(nx.Graph):

    def __init__(self, commits: List[Commit]):
        """
        Construct a mapping of developers committing to files.

        This function generates a NetworkX graph (`DevFileMapping`) that represents the relationship between
        developers and the files they have modified. The resulting graph consists of two types of nodes:

        - **Developers** (`type="dev"`)
        - **Files** (`type="file"`)

        Edges between developers and files indicate that a developer has modified a particular file in at least
        one commit. Each edge includes a `"commits"` attribute, which is a list of `mison.miner.Commit` objects
        representing the commits where the developer changed the file.

        ### Graph Properties:
        - **Nodes**: Each node has a `"type"` attribute set to either `"dev"` (developer) or `"file"` (file).
        - **Edges**: An edge exists between a developer and a file if the developer has modified that file.
          The `"commits"` attribute on the edge contains the list of related commits.

        :param commits: A list of mison.miner.Commit objects
        """
        super().__init__()
        for commit in commits:
            dev = commit.author_email
            self.add_node(dev, type='dev')
            for file in commit.modified_files:
                file = file.path
                self.add_node(file, type='file')
                if self.has_edge(dev, file):
                    self[dev][file]['commits'] += [commit]
                else:
                    self.add_edge(dev, file, commits=[commit])

    def map_developers(self, developer_mapping: Union[Mapping, Callable]):
        """
        Remap developers in a DevFileMapping or DevComponentMapping graph.

        This function updates a given DevFileMapping or DevComponentMapping `G` by replacing developer names
        according to the provided `developer_mapping`. Each occurrence of an old developer (`old_dev`) in `G`
        is replaced with a new developer (`new_dev = developer_mapping[old_dev]`), while preserving and
        reconnecting the links accordingly.

        If multiple developers are mapped to the same new developer, their links to files or components
        are merged under the new developer.

        ### Developer Mapping Options:
        - **Dictionary (`dict[old_dev, new_dev]`)**: Maps specific developers to new names. Developers not included
          in the dictionary remain unchanged.
        - **Function (`Callable[[old_dev], new_dev]`)**: A function that takes a developer name and returns the
          new name. If the function returns the same developer name, it remains unchanged.

        **Note:** The graph `G` is modified in place.

        :param developer_mapping: A dictionary or function mapping old developer names to new ones.
        :return: The modified `DevFileMapping` or `DevComponentMapping` graph with remapped developers.
        """
        devs, _ = split_bipartite_nodes(self, 'dev')
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
            if new_dev not in self:
                self.add_node(new_dev, type='dev')
            for _, file, data in self.edges(old_dev, data=True):
                self.add_edge(new_dev, file, **data)
            self.remove_node(old_dev)


    def map_renamed_files(self):
        """
        Map renamed files in a `DevFileMapping` graph to their latest filenames.

        This function updates a `DevFileMapping` graph by tracking file renames across commits and
        ensuring that all references to old filenames are mapped to their latest version.

        ### How It Works:
        - Resolve each file's latest name by scanning the commit history for file renames
            (`ModificationType.RENAME`) and records rename chains.
        - Updates the graph:
          - If an old filename has a newer version, it is **replaced** with the latest filename.
          - Merge edges from the old file node to the new file node while preserving commit data.
          - Store the list of old filenames under the `"old_paths"` attribute of the latest filename.

        ### Graph Updates:
        - **Nodes**: Old filenames are removed, and their data is merged into the latest filename node.
        - **Edges**: Developers previously linked to an old filename are reconnected to the latest filename.
        - **Attributes**: Each latest filename node retains a list of `"old_paths"` for traceability.

        :param self: A `DevFileMapping` graph
        :return: An updated `DevFileMapping` graph where all renamed files are mapped to their newest filenames.
        """
        files, _ = split_bipartite_nodes(self, 'file')
        rename_chain = dict()
        commits =  []
        for u, v, data in self.edges.data(data="commits"):
            for commit in data:
                commits.append(commit)
        commits = sorted(commits, key=lambda x: x.commit_date)
        for commit in commits:
                for modified_file in commit.modified_files:
                    if modified_file.modification_type == ModificationType.RENAME:
                        rename_chain[modified_file.old_path] = modified_file.new_path
                        if rename_chain.get(modified_file.new_path, None) == modified_file.old_path:
                            del rename_chain[modified_file.new_path]
        def reduce(key):
            while key in rename_chain:
                key = rename_chain[key]
            return key
        for old_file in files:
            newest_filename = reduce(old_file)
            if newest_filename == old_file:
                print(f"{old_file} is the newest filename")
                continue
            print(f"Mapping {old_file} to {newest_filename}")
            if newest_filename not in self:
                self.add_node(newest_filename, old_paths=[old_file])
            else:
                if "old_paths" in self.nodes[newest_filename]:
                    self.nodes[newest_filename]["old_paths"] += [old_file]
                else:
                    self.nodes[newest_filename]["old_paths"] = [old_file]
            for _, dev, data in self.edges(old_file, data=True):
                self.add_edge(newest_filename, dev, **data)
            self.remove_node(old_file)

    quick_clean_devs = quick_clean_devs


class DevComponentMapping(nx.Graph):
    quick_clean_devs = quick_clean_devs
    def __init__(self, G: DevFileMapping, component_mapping: Union[Mapping, Callable]):
        """
        Construct a `DevComponentMapping` graph from a `DevFileMapping` graph by grouping files into components.

        This function transforms a `DevFileMapping` graph into a `DevComponentMapping` graph by assigning files
        to components using the provided `component_mapping`. Each file in `DevFileMapping` is mapped to a
        component using `component_mapping(file)`. Developers will then be linked to components
        instead of individual files.

        ### Component Mapping Options:
        - **Dictionary (`dict[file, component]`)** mapping files to their corresponding components.
        - **Function (`Callable[[file], component]`)** returning the component for a given file.
        If a file is **not present in the dictionary** or if the function **returns `None`**, the file is
        **excluded**, and commits involving that file are omitted.

        ### Graph Structure:
        - **Nodes**:
          - Developers (`type="dev"`)
          - Components (`type="component"`)
        - **Edges**:
          - A developer is connected to a component if they have modified any file belonging to that component.
          - Each edge includes a `"commits"` attribute, which is a list of `mison.miner.Commit` objects representing
            all commits that modified any file mapped to the corresponding component.

        :param G: A `DevFileMapping` graph to be converted into a `DevComponentMapping` graph.
        :param component_mapping: A dictionary or function that maps files to components.
        :return: A `DevComponentMapping` graph with developers linked to components.
        """
        super().__init__()
        devs, files = split_bipartite_nodes(G, 'dev')
        self.add_nodes_from(devs, type='dev')
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
            if component not in self:
                self.add_node(component, type='component', files={file})
            else:
                self.nodes[component]["files"].update({file})
            for _, dev, data in G.edges(file, data=True):
                if dev in self.adj[component]:
                    self.adj[component][dev]["commits"].extend(data["commits"])
                else:
                    self.add_edge(dev, component, **data)


def split_bipartite_nodes(G: Union[DevFileMapping, DevComponentMapping], type):
    """
    Get two sets of nodes from a bipartite network.

    For a DevFileMapping or a DevComponentMapping, return two sets of nodes: nodes with "type" type and all others.
    :param G: A graph of either DevFileMapping or DevComponentMapping
    :param type: type of nodes to split over
    :return: top, bottom - top nodes are of "type" type and bottom are the rest
    """
    top = {n for n, d in G.nodes(data=True) if d["type"] == type}
    bottom = set(G) - top
    return top, bottom

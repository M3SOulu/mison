from mison.miner import Commit

from collections.abc import Mapping
from typing import Union, Callable, Iterable

import networkx as nx
from pydriller import ModificationType

__all__ = ['DevComponentMapping', 'DevFileMapping', 'DEV_STOP_LIST']

DEV_STOP_LIST = {"(none)", ""}


class DevFileMapping(nx.Graph):

    def __init__(self, *commits):
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

        :param commits: An Iterable, list of positional argument or a single instance of mison.miner.Commit objects
        """
        super().__init__()
        self._files = set()
        self._devs = set()
        if len(commits) > 0:
            self.add_commits(*commits)

    def add_commits(self, *commits):
        # If only one argument is passed
        if len(commits) == 1:
            obj = commits[0]
            if isinstance(obj, Iterable) and not isinstance(obj, (str, bytes)):
                # Single iterable provided (but not a string or bytes)
                for item in obj:
                    self._add_commit(item)
            elif isinstance(obj, Commit):
                # Single non-iterable object
                self._add_commit(obj)
        else:
            # Multiple positional arguments provided
            for obj in commits:
                self._add_commit(obj)

    def _add_commit(self, commit: Commit):
        dev = commit.author_email
        self.add_node(dev, type='dev')
        self._devs.add(dev)
        for file in commit.modified_files:
            file = file.path
            self.add_node(file, type='file')
            self._files.add(file)
            if self.has_edge(dev, file):
                self[dev][file]['commits'] += [commit]
            else:
                self.add_edge(dev, file, commits=[commit])

    @property
    def devs(self):
        return self._devs

    @property
    def files(self):
        return self._files

    def add_dev(self, dev: str):
        self.add_node(dev, type="dev")
        self._devs.add(dev)

    def remove_dev(self, dev: str):
        self.remove_node(dev)
        self._devs.remove(dev)

    def add_file(self, file: str):
        self.add_node(file, type="file")
        self._files.add(file)

    def remove_file(self, file: str):
        self.remove_node(file)
        self._files.remove(file)

    @property
    def components(self):
        return self._files

    def map_developers(self, developer_mapping: Union[Mapping, Callable]):
        """
        Remap developers in a DevFileMapping.

        This function updates the DevFileMapping by replacing developers
        according to the provided `developer_mapping`. Each occurrence of an old developer (`old_dev`)
        is replaced with a new developer (`new_dev = developer_mapping[old_dev]`), while preserving and
        reconnecting the links accordingly.

        If multiple developers are mapped to the same new developer, their links to files
        are merged under the new developer.

        ### Developer Mapping Options:
        - **Dictionary (`dict[old_dev, new_dev]`)**: Maps specific developers to new ones. Developers not included
          in the dictionary remain unchanged.
        - **Function (`Callable[[old_dev], new_dev]`)**: A function that takes a developer email and returns the
          new email. If the function returns the same developer email, the developer remains unchanged.

        :param developer_mapping: A dictionary or function mapping old developers to new ones.
        """
        devs = self._devs
        new_devs = set()
        if callable(developer_mapping):
            mapping_iter = map(developer_mapping, devs)
        elif isinstance(developer_mapping, Mapping):
            mapping_iter = map(lambda x: developer_mapping.get(x, x), devs)
        else:
            raise ValueError("developer_mapping must be a Mapping or a Callable")
        for old_dev, new_dev in zip(devs, mapping_iter):
            if old_dev == new_dev:
                print(f"Keeping {old_dev}")
                new_devs.add(old_dev)
                continue
            print(f"Replacing {old_dev} with {new_dev}")
            if new_dev not in self:
                self.add_node(new_dev, type='dev')
            for _, file, data in self.edges(old_dev, data=True):
                self.add_edge(new_dev, file, **data)
            self.remove_node(old_dev)
            new_devs.add(new_dev)
        self._devs = new_devs


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
        files = self._files
        file_to_chain = dict()
        commits = set()
        for u, v, data in self.edges.data(data="commits"):
            commits.update(data)
        commits = sorted(commits, key=lambda x: x.commit_date)
        for commit in commits:
            for modified_file in commit.modified_files:
                if modified_file.modification_type == ModificationType.RENAME:
                    old = modified_file.old_path
                    new = modified_file.new_path

                    if old in file_to_chain:
                        # Continue an existing chain
                        chain = file_to_chain[old]
                        chain.append(new)

                        # Update the mapping for the new file
                        file_to_chain[new] = chain
                    else:
                        # Start a new chain
                        chain = [old, new]
                        file_to_chain[new] = chain
                        file_to_chain[old] = chain  # Reference old name too
        for chain in file_to_chain.values():
            newest_filename = chain[-1]
            for old_file in chain[:-2]:
                print(f"Mapping {old_file} to {newest_filename}")
                if newest_filename not in self:
                    self.add_node(newest_filename, old_paths=[old_file])
                    self._files.add(newest_filename)
                else:
                    if "old_paths" in self.nodes[newest_filename]:
                        self.nodes[newest_filename]["old_paths"] += [old_file]
                    else:
                        self.nodes[newest_filename]["old_paths"] = [old_file]
                for _, dev, data in self.edges(old_file, data=True):
                    self.add_edge(newest_filename, dev, **data)
                if old_file in self:
                    self.remove_node(old_file)
                    self._files.remove(old_file)

    def quick_clean_devs(self):
        """
        Remove developers who found in a common stoplist.

        :param self: A graph of either DevComponentMapping or DevFileMapping
        :return: The filtered graph (graph is modified in-place)
        """
        nodes_remove = {node for node in self._devs if node in DEV_STOP_LIST}
        for node in nodes_remove:
            print(f"Found {node}; to be removed")
        self.remove_nodes_from(nodes_remove)


class DevComponentMapping(nx.Graph):
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
        self._components = set()
        devs, files = G.devs, G.files
        self._devs = devs
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
                self._components.add(component)
            else:
                self.nodes[component]["files"].update({file})
            for _, dev, data in G.edges(file, data=True):
                if dev in self.adj[component]:
                    self.adj[component][dev]["commits"].extend(data["commits"])
                else:
                    self.add_edge(dev, component, **data)

    @property
    def components(self):
        return self._components

    @property
    def devs(self):
        return self._devs


def _split_bipartite_nodes(G: Union[DevFileMapping, DevComponentMapping], type):
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

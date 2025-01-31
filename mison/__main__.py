from .mine import pydriller_mine_commits, github_mine_commits, CommitJSONEncoder
from .network import construct_bipartite, developer_collaboration_network, map_developers, quick_clean_devs

import pandas
import networkx as nx

import argparse
import datetime
import importlib.util
import os
import sys
import json


def import_microservice_mapping(filename: str, funcname: str = None):

    if filename is None:
        return None
    elif filename.startswith('mison.mappings'):
        module = importlib.import_module(filename)
        return module.microservice_mapping

    # Add the directory of the file to sys.path
    dir_name = os.path.dirname(filename)
    if dir_name not in sys.path:
        sys.path.append(dir_name)

    if funcname is None:
        funcname = 'microservice_mapping'

    # Import the module
    spec = importlib.util.spec_from_file_location(funcname, filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.microservice_mapping


def main_commit(args):
    if args.backend == 'pydriller':
        pydriller_kwargs = {'since': args.since,
                            'from_commit': args.from_commit,
                            'from_tag': args.from_tag,
                            'to': args.to,
                            'to_commit': args.to_commit,
                            'to_tag': args.to_tag,
                            'order': args.order,
                            'only_in_branch': args.only_in_branch,
                            'only_no_merge': args.only_no_merge,
                            'only_authors': args.only_authors,
                            'only_commits': args.only_commits,
                            'only_releases': args.only_releases,
                            'filepath': args.filepath,
                            'only_modifications_with_file_types': args.only_modifications_with_file_types
                            }
        pydriller_mine_commits(repo=args.repo, output=args.commit_table, **pydriller_kwargs)
    elif args.backend == 'github':
        github_mine_commits(repo=args.repo, github_token=args.github_token, output=args.commit_table,
                            per_page=args.per_page)


def main_network(args):
    data = pandas.read_csv(args.commit_table)
    if args.developer_mapping is not None:
        if args.developer_mapping.endswith(".py"):
            mapping = import_microservice_mapping(args.developer_mapping, "developer_mapping")
        elif args.developer_mapping.endswith(".json"):
            with open(args.developer_mapping, 'r') as f:
                mapping = json.load(f)
        else:
            raise ValueError("--developer_mapping must be a .py or .json file")
    G = construct_bipartite(data)
    if args.quick_clean:
        G = quick_clean_devs(G)
    if args.developer_mapping is not None:
        G = map_developers(G, mapping)
    D = developer_collaboration_network(G)
    net = nx.node_link_data(D, link="edges")
    with open(args.network_output, 'w') as f:
        json.dump(net, f, cls=CommitJSONEncoder, indent=4)


def main():

    if len(sys.argv) == 1:
            sys.argv.append('-h')

    # Main parser
    parser = argparse.ArgumentParser(description='MiSON - MicroService Organisational Network miner',
                                     prog="MiSON")

    # Common commit parameters
    commit = argparse.ArgumentParser(description='Mine commits of a repository with PyDriller', add_help=False)
    commit.add_argument('--repo', type=str, required=True, help='Path to the repository (local path or URL)')
    commit.add_argument('--backend', choices=['pydriller', 'github'], required=True, help='Available backends for commit mining')
    commit.add_argument('--commit_table', type=str, required=True,
                            help='Output path for the csv table of mined commits')

    # Filters for PyDriller
    pydriller = commit.add_argument_group('PyDriller backend parameters', 'Parameters for mining commits with PyDriller backend')
    # FROM
    pydriller.add_argument('--since', required=False, type=datetime.datetime.fromisoformat,
                        help='Only commits after this date will be analyzed (converted to datetime object)')
    pydriller.add_argument('--from_commit', required=False, type=str,
                        help='Only commits after this commit hash will be analyzed')
    pydriller.add_argument('--from_tag', required=False, type=str, help='Only commits after this commit tag will be analyzed')
    # TO
    pydriller.add_argument('--to', required=False, type=datetime.datetime.fromisoformat,
                      help='Only commits up to this date will be analyzed (converted to datetime object)')
    pydriller.add_argument('--to_commit', required=False, type=str, help='Only commits up to this commit hash will be analyzed')
    pydriller.add_argument('--to_tag', required=False, type=str, help='Only commits up to this commit tag will be analyzed')
    # Filters
    pydriller.add_argument('--order', required=False, choices=['date-order', 'author-date-order', 'topo-order', 'reverse'])
    pydriller.add_argument('--only_in_branch', required=False, type=str,
                           help='Only analyses commits that belong to this branch')
    pydriller.add_argument('--only_no_merge', required=False, action='store_true',
                           help='Only analyses commits that are not merge commits')
    pydriller.add_argument('--only_authors', required=False, nargs='*',
                           help='Only analyses commits that are made by these authors')
    pydriller.add_argument('--only_commits', required=False, nargs='*', help='Only these commits will be analyzed')
    pydriller.add_argument('--only_releases', required=False, action='store_true',
                           help='Only commits that are tagged (“release” is a term of GitHub, does not actually exist in Git)')
    pydriller.add_argument('--filepath', required=False, type=str,
                           help='Only commits that modified this file will be analyzed')
    pydriller.add_argument('--only_modifications_with_file_types', required=False, nargs='*',
                           help='Only analyses commits in which at least one modification was done in that file type')

    # Parameters for GitHub API
    github = commit.add_argument_group('GitHub backend parameters', 'Parameters for mining commits with GitHub backend')
    github.add_argument('--github_token', default=None, required=False, help='GitHub API token for mining data.'
                                                                             'Can also be provided as env. GITHUB_TOKEN')
    github.add_argument('--per_page', type=int, default=100, help='How many commits per page request from GitHub API')

    # Network parameters
    network = argparse.ArgumentParser(description='Construct a developer network from a commit table', add_help=False)
    network.add_argument('--network_output', type=str, required=False, help='Output path for network')
    network.add_argument('--commit_table', type=str, required=True, help='Input path of the csv table of mined commits')
    network.add_argument('--quick_clean', action='store_true', help='If set, use pre-defined stop-list to remove developer nodes')
    network.add_argument('--developer_mapping', type=str, required=False,
                        help='File to import developer mapping from. Can be a .py file which defines '
                             "a function 'developer_mapping'"
                             "or a .json files with a dictionary")

    # Sub-commands for main
    subparsers = parser.add_subparsers(required=True)

    # Commit command
    commit_sub = subparsers.add_parser('commit', parents=[commit], help='Mine commits of a repository with PyDriller',
                                       conflict_handler='resolve')
    commit_sub.set_defaults(func=main_commit)

    # Network command
    network_sub = subparsers.add_parser('network', parents=[network],
                                        help='Construct a developer network from a commit table',
                                        conflict_handler='resolve')
    network_sub.set_defaults(func=main_network)

    # Parse the arguments
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()

import datetime
import argparse
import os
import importlib.util
import sys
import itertools

import pandas
from pydriller import Repository, Commit, ModifiedFile
import pandas as pd


def import_microservice_mapping(filename):

    if filename is None:
        return None

    # Add the directory of the file to sys.path
    dir_name = os.path.dirname(filename)
    if dir_name not in sys.path:
        sys.path.append(dir_name)

    # Import the module
    spec = importlib.util.spec_from_file_location('microservice_mapping', filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.microservice_mapping


def construct_network(commit_table, field='file', output=None):

    assert field in ('file', 'service')

    devs = {}
    for row in commit_table.itertuples(index=False):
        dev = devs.setdefault(row.author_name, set())
        f = row.filename if field == 'file' else row.microservice
        if pd.notna(f):
            dev.add(f)

    ordered_pairs = itertools.product(devs.keys(), repeat=2)
    unordered_pairs = {(a,b) for (a,b) in ordered_pairs if a < b}

    filecounts = [(dev_a, dev_b, len(devs[dev_a] & devs[dev_b])) for dev_a, dev_b in unordered_pairs]
    filecounts = pd.DataFrame(filecounts, columns=['developer_a', 'developer_b', 'weight'])

    if output is not None:
        if output == 'default':
            output = f"mison_developer_network_{field}_{datetime.datetime.now().isoformat()}.csv"
        filecounts.to_csv(output, index=False)

    return filecounts


def mine_commits(repo, branch, output=None, mapping=None):

    data = []

    for commit in Repository(repo, only_in_branch=branch).traverse_commits():
        for file in commit.modified_files:
            data.append([commit.hash, commit.author.name, commit.author.email, commit.committer.name,
                         commit.committer.email, commit.committer_date, file.added_lines, file.deleted_lines,
                         file.new_path])

    data = pd.DataFrame(data, columns=['commit_hash', 'author_name', 'author_email', 'committer_name', 'committer_email',
                                       'commit_date', 'additions', 'deletions', 'filename'])

    if mapping is not None:
        data['microservice'] = data['filename'].map(mapping)

    if output is not None:
        if output == 'default':
            output = f"mison_commit_table_{datetime.datetime.now().isoformat()}.csv"
        data.to_csv(output, index=False)

    return data


if __name__ == '__main__':

    def main_commit(args):
        microservice_mapping = import_microservice_mapping(args.import_mapping)
        data = mine_commits(repo=args.repo, branch=args.branch, output=args.commit_table, mapping=microservice_mapping)
        return data

    def main_network(args):
        data = pandas.read_csv(args.commit_table)
        construct_network(data, args.field, output=args.network_output)

    def main_all(args):
        data = main_commit(args)
        construct_network(data, args.field, output=args.network_output)


    # Main parser
    parser = argparse.ArgumentParser(description='MiSON - MicroService Organisational Network miner')

    # Common commit parameters
    commit = argparse.ArgumentParser(description='Mine commits of a repository with PyDriller', add_help=False)
    commit.add_argument('--branch', type=str, required=True, help='Name of the branch to mine')
    commit.add_argument('--repo', type=str, required=True, help='Path to the repository (local path or URL)')
    commit.add_argument('--import_mapping', type=str, required=False, help='Python file to import a microservice_mapping function from')

    # Common network parameters
    network = argparse.ArgumentParser(description='Construct a developer network from a commit table', add_help=False)
    network.add_argument('--field', choices=['file', 'service'], required=True, help='Which field to use for network weight')
    network.add_argument('--network_output', type=str, required=False, help='Output path for network')

    # Sub-commands for main
    subparsers = parser.add_subparsers(required=True)

    # Commit command
    commit_sub = subparsers.add_parser('commit', parents=[commit], help='Mine commits of a repository with PyDriller', conflict_handler='resolve')
    commit_sub.add_argument('--commit_table', type=str, required=True, help='Output path for the csv table of mined commits')
    commit_sub.set_defaults(func=main_commit)

    # Network command
    network_sub = subparsers.add_parser('network', parents=[network], help='Construct a developer network from a commit table', conflict_handler='resolve')
    # Network only needs input file if called separately
    network_sub.add_argument('--commit_table', type=str, required=True, help='Input path of the csv table of mined commits')
    network_sub.set_defaults(func=main_network)

    # End-to-end command
    end_to_end = subparsers.add_parser('all', parents=[commit, network], help='End-to-end network generation from the repository', conflict_handler='resolve')
    end_to_end.add_argument('--commit_table', type=str, required=False, help='Output path for the csv table of mined commits')
    end_to_end.set_defaults(func=main_all)

    # Parse the arguments
    args = parser.parse_args()
    args.func(args)

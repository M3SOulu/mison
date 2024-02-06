import datetime
import argparse
import os
import importlib.util
import sys
import itertools

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


def construct_network(filename, output=None):
    if output is None:
        output = f"mison_developer_network_{datetime.datetime.now().isoformat()}.csv"

    devs = {}
    data = pd.read_csv(filename, index_col=False)
    for row in data.itertuples(index=False):
        dev = devs.setdefault(row.author_name, set())
        if pd.notna(row.filename):
            dev.add(row.filename)

    ordered_pairs = itertools.product(devs.keys(), repeat=2)
    unordered_pairs = {(a,b) for (a,b) in ordered_pairs if a < b}

    filecounts = [(dev_a, dev_b, len(devs[dev_a] & devs[dev_b])) for dev_a, dev_b in unordered_pairs]
    filecounts = pd.DataFrame(filecounts, columns=['developer_a', 'developer_b', 'weight'])

    filecounts.to_csv(output, index=False)

    return filecounts

def mine_commits(repo, branch, output=None, mapping=None):
    if output is None:
        output = f"mison_commits_mined_{datetime.datetime.now().isoformat()}.csv"

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

    data.to_csv(output, index=False)

    return data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MiSON - MicroService Organisational Network miner')

    # Add the positional arguments
    parser.add_argument('--branch', type=str, required=True, help='Name of the branch to mine')
    parser.add_argument('--repo', type=str, required=True, help='Path to the repository (local path or URL)')
    parser.add_argument('--commit_table', type=str, required=False, help='Output path for the csv table of mined commits')
    parser.add_argument('--import_mapping', type=str, required=False, help='Python file to import a microservice_mapping function from')

    # Parse the arguments
    args = parser.parse_args()
    #microservice_mapping = import_microservice_mapping(args.import_mapping)
    #mine_commits(repo=args.repo, branch=args.branch, output=args.commit_table, mapping=microservice_mapping)
    construct_network('2024.csv')
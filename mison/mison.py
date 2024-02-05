import datetime
import argparse
import os

from pydriller import Repository, Commit, ModifiedFile
import pandas as pd


def trainticket_mappiing(filename):
    if filename is None:
        return None
    service = str(filename).split(os.sep)[0]
    if service.startswith('ts-') and "service" in service:
        return service
    else:
        return None


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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MiSON - MicroService Organisational Network miner')

    # Add the positional arguments
    parser.add_argument('--branch', type=str, required=True, help='Name of the branch to mine')
    parser.add_argument('--repo', type=str, required=True, help='Path to the repository (local path or URL)')
    parser.add_argument('--commit_table', type=str, required=False, help='Output path for the csv table of mined commits')

    # Parse the arguments
    args = parser.parse_args()
    mine_commits(repo=args.repo, branch=args.branch, output=args.commit_table, mapping=trainticket_mappiing)
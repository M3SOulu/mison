import datetime
import os
import importlib.util
import sys
import itertools

from pydriller import Repository
import pandas as pd

__all__ = ['import_microservice_mapping', 'construct_network', 'mine_commits']


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


def construct_network(commit_table, field='file', output=None, skip_zero=False):

    assert field in ('file', 'service')

    devs = {}
    for row in commit_table.itertuples(index=False):
        dev = devs.setdefault(row.author_email, set())
        f = row.filename if field == 'file' else row.microservice
        if pd.notna(f):
            dev.add(f)

    ordered_pairs = itertools.product(devs.keys(), repeat=2)
    unordered_pairs = {(a,b) for (a,b) in ordered_pairs if a < b}

    filecounts = [(dev_a, dev_b, len(devs[dev_a] & devs[dev_b])) for dev_a, dev_b in unordered_pairs]
    filecounts = pd.DataFrame(filecounts, columns=['developer_a', 'developer_b', 'weight'])
    filecounts = filecounts[filecounts.weight != 0] if skip_zero else filecounts

    if output is not None:
        if output == 'default':
            output = f"mison_developer_network_{field}_{datetime.datetime.now().isoformat()}.csv"
        filecounts.to_csv(output, index=False)

    return filecounts


def mine_commits(repo, output=None, mapping=None, **kwargs):

    pydriller_kwargs = {k: v for k, v in kwargs.items() if v is not None}
    data = []

    for commit in Repository(repo, **pydriller_kwargs).traverse_commits():
        for file in commit.modified_files:
            data.append([commit.hash, commit.author.name, commit.author.email.lower(), commit.committer.name,
                         commit.committer.email.lower(), commit.committer_date, file.added_lines, file.deleted_lines,
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
    print('ERROR - run this module as main as "python -m mison')

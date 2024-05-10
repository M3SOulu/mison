import datetime
import os
import itertools

import requests

from pydriller import Repository
import pandas as pd

__all__ = ['construct_network', 'pydriller_mine_commits', 'github_mine_commits']


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


def pydriller_mine_commits(repo, output=None, mapping=None, **kwargs):

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
            output = f"mison_pydriller_commit_table_{datetime.datetime.now().isoformat()}.csv"
        data.to_csv(output, index=False)

    return data


def github_mine_commits(repo: str, github_token=None, mapping=None, output=None, per_page=100):

    if github_token is None:
        github_token = os.getenv('GITHUB_TOKEN')
        if github_token is None:
            raise ValueError("GitHub token needs to be provided either as a function/cli argument or in env. var. GITHUB_TOKEN")

    repo = repo.removeprefix('https://github.com/')
    project_commits_query = f"https://api.github.com/repos/{repo}/commits"
    headers = {'Authorization': f'token {github_token}'}
    params = {'per_page': per_page}

    data = []
    page = 1
    while 1 == 1:
        params['page'] = page
        project_commits_result = requests.get(project_commits_query, headers=headers, params=params)
        project_commits_data: list[dict] = project_commits_result.json()
        if len(project_commits_data) == 0:
            break
        for item in project_commits_data:
            commit_hash = item['sha']
            commit_data = [commit_hash]  # commit_hash
            if 'commit' in item:
                if 'author' in item:
                    commit_data.append(item['commit']['author'].get('name', None))  # author_name
                    commit_data.append(item['commit']['author'].get('email', None))  # author_email
                else:
                    commit_data.extend([None]*2)
                if 'committer' in item:
                    commit_data.append(item['commit']['committer'].get('name', None))  # committer_name
                    commit_data.append(item['commit']['committer'].get('email', None))  # committer_email
                    commit_data.append(item['commit']['committer']['date'])  # commit_date
                else:
                    commit_data.extend([None]*3)
            else:
                commit_data.extend([None]*5)
            commit_changes_query = f'{project_commits_query}/{commit_hash}'
            commit_changes_response = requests.get(commit_changes_query, headers=headers)
            commit_changes_data = commit_changes_response.json()
            changed_files = commit_changes_data['files']
            for file in changed_files:
                file_commit_data = commit_data.copy()
                file_commit_data.extend([file['additions'], file['deletions'], file['filename']]) # additions, deletions, filename
                data.append(file_commit_data)
        page += 1
    columns = ['commit_hash', 'author_name', 'author_email', 'committer_name', 'committer_email', 'commit_date',
               'additions', 'deletions', 'filename']
    data = pd.DataFrame(data, columns=columns)

    if mapping is not None:
        data['microservice'] = data['filename'].map(mapping)

    if output is not None:
        if output == 'default':
            output = f"mison_github_commit_table_{datetime.datetime.now().isoformat()}.csv"
        data.to_csv(output, index=False)

    return data


if __name__ == '__main__':
    print('ERROR - run this module as main as "python -m mison')

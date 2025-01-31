import os
import requests
from datetime import datetime
from dataclasses import dataclass
from json import JSONEncoder

from pydriller import Repository
import pandas as pd

__all__ = ['pydriller_mine_commits', 'github_mine_commits', 'Commit', 'CommitJSONEncoder']

@dataclass
class Commit:
    sha: str
    author_name: str
    author_email: str
    committer_name: str
    committer_email: str
    commit_date: datetime
    filename: str
    additions: int = 0
    deletions: int = 0

class CommitJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Commit):
            return o.__dict__
        else:
            return super().default(o)

def pydriller_mine_commits(repo, **kwargs):
    """
    Mining git repository commits and file modifications with PyDriller library
    :param repo: str, path to the repository folder (can be online, will be temporarily cloned)
    :param kwargs: kwargs for pydriller.Repository (filters, commits range)
    :return: pandas DataFrame with all mined commits and file modifications
    """

    pydriller_kwargs = {k: v for k, v in kwargs.items() if v is not None}
    data = []

    for commit in Repository(repo, **pydriller_kwargs).traverse_commits():
        for file in commit.modified_files:
            data.append([commit.hash, commit.author.name, commit.author.email.lower(), commit.committer.name,
                         commit.committer.email.lower(), commit.committer_date, file.added_lines, file.deleted_lines,
                         file.new_path])

    data = pd.DataFrame(data, columns=['commit_hash', 'author_name', 'author_email', 'committer_name', 'committer_email',
                                       'commit_date', 'additions', 'deletions', 'filename'])
    return data


def github_mine_commits(repo: str, github_token=None, per_page=100):
    """
    Mining git repository commits and file modifications with GitHub API.
    :param repo: str, address of the repository on GitHub
    :param github_token: str, the GitHub API token to use for API access; if None, will try to get GITHUB_TOKEN env
    :param per_page: (optional) amount of commits to return per page, passed to the GitHub API request
    :return: pandas DataFrame with all mined commits and file modifications
    :raise ValueError: if the GitHub API is not provided neither as parameter not environment variable
    """

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

    return data


if __name__ == '__main__':
    print('ERROR - run this module as main as "python -m mison')

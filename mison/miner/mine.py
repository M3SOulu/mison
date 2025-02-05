import os
import requests
from datetime import datetime
from dataclasses import dataclass
from json import JSONEncoder
from typing import List

from pydriller import Repository

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
        elif isinstance(o, datetime):
            return o.isoformat()
        else:
            return super().default(o)

def pydriller_mine_commits(repo, **kwargs) -> List[Commit]:
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
            data.append(Commit(commit.hash, commit.author.name, commit.author.email.lower(), commit.committer.name,
                         commit.committer.email.lower(), commit.committer_date, file.new_path, file.deleted_lines,
                         file.added_lines))

    return data


def github_mine_commits(repo: str, github_token=None, per_page=100) -> List[Commit]:
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

    commits_data = []
    page = 1

    while True:
        params['page'] = page
        response = requests.get(project_commits_query, headers=headers, params=params)
        project_commits_data: list[dict] = response.json()

        if not project_commits_data:
            break

        for item in project_commits_data:
            commit_sha = item['sha']
            author_name: str = item.get('commit', {}).get('author', {}).get('name', None)
            author_email: str = item.get('commit', {}).get('author', {}).get('email', None)
            committer_name: str = item.get('commit', {}).get('committer', {}).get('name', None)
            committer_email: str = item.get('commit', {}).get('committer', {}).get('email', None)
            commit_date: str = item.get('commit', {}).get('committer', {}).get('date', None)

            if commit_date:
                commit_date: datetime = datetime.fromisoformat(commit_date.replace("Z", "+00:00"))

            # Fetch detailed commit changes
            commit_changes_query = f"{project_commits_query}/{commit_sha}"
            commit_changes_response = requests.get(commit_changes_query, headers=headers)
            commit_changes_data = commit_changes_response.json()

            for file in commit_changes_data.get("files", []):
                commit_entry = Commit(
                    sha=commit_sha,
                    author_name=author_name,
                    author_email=author_email,
                    committer_name=committer_name,
                    committer_email=committer_email,
                    commit_date=commit_date,
                    filename=file.get("filename"),
                    additions=file.get("additions", 0),
                    deletions=file.get("deletions", 0))
                commits_data.append(commit_entry)

        page += 1

    return commits_data

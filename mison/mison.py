import csv
import datetime
import os
import importlib.util
import sys
import itertools

import numpy as np
import requests

from pydriller import Repository
import pandas as pd

__all__ = ['import_microservice_mapping', 'construct_network', 'mine_commits']

from commitsCrawler import headers


def import_microservice_mapping(filename: str):

    if filename is None:
        return None
    elif filename.startswith('mison.mappings'):
        module = importlib.import_module(filename)
        return module.microservice_mapping

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


def getCommitTablebyProject(repo, github_token=None):

    if github_token is None:
        github_token = os.getenv('GITHUB_TOKEN')
        if github_token is None:
            raise ValueError("GitHub token needs to be provided either as a function/cli argument or in env. var. GITHUB_TOKEN")
    headers = {'Authorization': f'token {github_token}'}

    def getFileChanges(projectfullname, thecommitcsv, newcsv):
        commit_df = pd.read_csv(thecommitcsv)
        commit_features = ['project_id', 'commit_sha', 'author_name', 'committer_name', 'commit_date', 'additions',
                           'deletions', 'changes', 'filename']
        with open(newcsv, 'a', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(commit_features)
        for i in range(commit_df.shape[0]):
            print(i)
            thecommit = commit_df.iloc[i].values.tolist()
            commit_sha = thecommit[1]
            theCommitShaQuery = f"https://api.github.com/repos/{projectfullname}/commits/" + commit_sha
            sha_result = requests.get(theCommitShaQuery, headers=headers)
            commit_info = sha_result.json()
            changed_files = [[x['additions'], x['deletions'], x['changes'], x['filename']] for x in
                             commit_info['files']]
            for file in changed_files:
                with open(newcsv, 'a', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',')
                    writer.writerow(thecommit + list(file))

    theCommitQuery = f"https://api.github.com/repos/{repo}/commits"
    theProjectQuery = f"https://api.github.com/repos/{repo}"
    p_search = requests.get(theProjectQuery, headers=headers)
    project_info = p_search.json()
    project_id = project_info['id']
    params = {'per_page': 100}
    page = 1
    #projectissuedataitems = []
    commit_features = ['project_id', 'commit_sha', 'author_name', 'committer_name', 'commit_date']
    with open(updateissuetablename, 'a', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(commit_features)
    while 1 == 1:
        params['page'] = page
        print(page)
        print(projectfullname + ' ' + 'page ' + str(page))
        theResult = requests.get(theCommitQuery, headers=headers, params=params)
        theItemListPerPage = theResult.json()
        if len(theItemListPerPage) == 0:
            break
        else:
            print(len(theItemListPerPage))
            for item in theItemListPerPage:
                commititem = {}
                commititem['project_id'] = project_id
                commititem['commit_sha'] = item['sha']
                try:
                    commititem['author_name'] = item['commit']['author']['name']
                except:
                    commititem['author_name'] = np.NaN
                try:
                    commititem['committer_name'] = item['commit']['committer']['name']
                except:
                    commititem['committer_name'] = np.NaN
                commititem['commit_date'] = item['commit']['committer']['date']

                with open(updateissuetablename, 'a', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',')
                    writer.writerow([commititem[x] for x in commit_features])
            page = page + 1


if __name__ == '__main__':
    print('ERROR - run this module as main as "python -m mison')



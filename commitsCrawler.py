import os
import requests
import pandas as pd
import numpy as np
import csv, json


def getIssueTablebyProject(projectfullname, updateissuetablename): #, updatelabeltablename):
    theIssueQuery = f"https://api.github.com/repos/{projectfullname}/issues"
    theProjectQuery = f"https://api.github.com/repos/{projectfullname}"
    p_search = requests.get(theProjectQuery, headers=headers)
    project_info = p_search.json()
    project_id = project_info['id']
    issue_feature_list = ['project_id', 'issue_count', 'unique_user_count', 'type_user_count', 'type_organization_count', 'type_bot_count', 'open_count', 'closed_count', 'locked_count', 'average_active_time', 'milestone_related_count', 'average_comments']
    label_feature_list = ['project_id', 'issue_id', 'label_id', 'name', 'color', 'default', 'description']
    #issueheader = ','.join(issue_feature_list)
    #labelheader = ','.join(label_feature_list)
    #with open(updateissuetablename, 'a', encoding='utf-8') as csvfile:
    #    writer = csv.writer(csvfile, delimiter=',')
    #    writer.writerow(issueheader)
    #with open(updatelabeltablename, 'a', encoding='utf-8') as csvfile:
    #    writer = csv.writer(csvfile, delimiter=',')
    #    writer.writerow(labelheader)
    params = {'per_page': 100, 'state': 'all'}
    #theOpenIssueList = []
    page = 1
    projectissuedataitems = []
    thereturnvalues = {}
    labeldataitems = []
    while 1 == 1:
        params['page'] = page
        print(projectfullname+' '+'page '+str(page))
        theResult = requests.get(theIssueQuery, headers=headers, params=params)
        theItemListPerPage = theResult.json()
        if len(theItemListPerPage) == 0:
            break
        else:
            for item in theItemListPerPage:
                issueitem = {}
                issueitem['project_id'] = project_id
                issueitem['issue_id'] = item['id']
                issueitem['number'] = item['number']
                issueitem['user_login'] = item['user']['login']
                issueitem['user_type'] = item['user']['type']
                issueitem['state'] = item['state']
                issueitem['locked'] = item['locked']
                issueitem['created_at'] = (pd.to_datetime(item['created_at'], utc=True)- pd.Timestamp("1970-01-01").tz_localize('UTC'))// pd.Timedelta('1s')
                if item['updated_at'] == None:
                    issueitem['updated_at'] = np.NaN
                else:
                    issueitem['updated_at'] = (pd.to_datetime(item['updated_at'], utc=True)- pd.Timestamp("1970-01-01").tz_localize('UTC'))// pd.Timedelta('1s')
                if item['closed_at'] == None:
                    issueitem['closed_at'] = np.NaN
                else:
                    issueitem['closed_at'] = (pd.to_datetime(item['closed_at'], utc=True)- pd.Timestamp("1970-01-01").tz_localize('UTC'))// pd.Timedelta('1s')
                if issueitem['state'] == 'closed':
                    issueitem['active_time'] = issueitem['closed_at'] - issueitem['created_at']
                else:
                    if not issueitem['updated_at']:
                        issueitem['active_time'] = 0
                    else:
                        issueitem['active_time'] = issueitem['updated_at'] - issueitem['created_at']
                #issueitem['title'] = item['title']
                #issueitem['body'] = item['body']
                if item['milestone'] == None:
                    issueitem['milestone_url'] = False
                else:
                    issueitem['milestone_url'] = True
                issueitem['comments'] = item['comments']
                if 'pull_request' in list(item.keys()):
                    issueitem['pull_request'] = True
                else:
                    issueitem['pull_request'] = False
                #with open(updateissuetablename, 'a', encoding='utf-8') as csvfile:
                #    writer = csv.writer(csvfile, delimiter=',')
                #    writer.writerow([issueitem[x] for x in issue_feature_list])
                projectissuedataitems.append(issueitem)
                ############# get labels ################
                #if item['labels'] == None:
                #    pass
                #else:
                #    for label in item['labels']:
                #        labelitem = {}
                #        labelitem['project_id'] = project_id
                #        labelitem['issue_id'] = item['id']
                #        labelitem['label_id'] = label['id']
                #        labelitem['name'] = label['name']
                #        labelitem['color'] = label['color']
                #        labelitem['default'] = label['default']
                #        if label['description'] == None:
                #            labelitem['description'] = np.NaN
                #        else:
                #            labelitem['description'] = label['description']
                #        labeldataitems.append(labelitem)
                #        with open(updatelabeltablename, 'a', encoding='utf-8') as csvfile:
                #            writer = csv.writer(csvfile, delimiter=',')
                #            writer.writerow([labelitem[x] for x in label_feature_list])
                ###########################################
            #theOpenIssueList.extend(theItemListPerPage)
            page = page + 1
        #issue_feature_list = ['project_id', 'issue_count', 'unique_user_count', 'type_user_count', 'type_organization_count', 'type_bot_count',
        #                      'open_count', 'closed_count', 'locked_count', 'average_active_time',
        #                      'milestone_related_count', 'average_comments', 'pull_request_count']
    thereturnvalues['project_id'] = project_id
    thereturnvalues['issue_count'] = len(projectissuedataitems)
    thereturnvalues['unique_user_count'] = len(list(set([x['user_login'] for x in projectissuedataitems])))
    thereturnvalues['type_user_count'] = len([x['user_type'] for x in projectissuedataitems if x['user_type']=='User'])
    thereturnvalues['type_organization_count'] = len([x['user_type'] for x in projectissuedataitems if x['user_type']=='Organization'])
    thereturnvalues['type_bot_count'] = len([x['user_type'] for x in projectissuedataitems if x['user_type']=='Bot'])
    thereturnvalues['open_count'] = len([x['state'] for x in projectissuedataitems if x['state']=='open'])
    thereturnvalues['closed_count'] = len([x['state'] for x in projectissuedataitems if x['state']=='closed'])
    thereturnvalues['locked_count'] = sum([x['locked'] for x in projectissuedataitems])
    thereturnvalues['average_active_time'] = sum([x['active_time'] for x in projectissuedataitems])/len(projectissuedataitems)
    thereturnvalues['average_closed_time'] = sum([x['active_time'] for x in projectissuedataitems if x['state']=='closed'])/len(projectissuedataitems)
    thereturnvalues['milestone_related_count'] = sum([x['milestone_url'] for x in projectissuedataitems])
    thereturnvalues['average_comments'] = sum([x['comments'] for x in projectissuedataitems])/len(projectissuedataitems)
    thereturnvalues['pull_request_count'] = sum([x['pull_request'] for x in projectissuedataitems])
    features = list(thereturnvalues.keys())
    currentdf = pd.read_csv(updateissuetablename, index_col='project_id')
    existingprojects = currentdf.index.values.tolist()
    #existingprojects = []
    if project_id in existingprojects:
        currentdf.loc[project_id] = [thereturnvalues[x] for x in features][1:]
        currentdf.reset_index(inplace=True)
        currentdf.to_csv(updateissuetablename, encoding='utf-8', index=False)
    else:
        with open(updateissuetablename, 'a', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow([thereturnvalues[x] for x in features])


def getCommitTablebyProject(projectfullname, updateissuetablename):
    theCommitQuery = f"https://api.github.com/repos/{projectfullname}/commits"
    theProjectQuery = f"https://api.github.com/repos/{projectfullname}"
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
                #commititem['message'] = item['commit']['message']

                with open(updateissuetablename, 'a', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',')
                    writer.writerow([commititem[x] for x in commit_features])
                #projectissuedataitems.append(commititem)
            page = page + 1

#getCommitTablebyProject("FudanSELab/train-ticket", "commits.csv")

def getFileChanges(projectfullname, thecommitcsv, newcsv):
    commit_df = pd.read_csv(thecommitcsv)
    committers = commit_df.loc[:, 'committer_name'].values.tolist()
    committers = list(set([x for x in committers if str(x) != 'nan']))
    authors= commit_df.loc[:, 'author_name'].values.tolist()
    authors = list(set([x for x in authors if str(x) != 'nan']))
    users = list(set(committers+authors))
    commit_features = ['project_id', 'commit_sha', 'author_name', 'committer_name', 'commit_date', 'additions', 'deletions', 'changes', 'filename']
    with open(newcsv, 'a', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(commit_features)
    for i in range(commit_df.shape[0]):
        print(i)
        thecommit = commit_df.iloc[i].values.tolist()
        commit_sha = thecommit[1]
        theCommitShaQuery = f"https://api.github.com/repos/{projectfullname}/commits/"+commit_sha
        sha_result = requests.get(theCommitShaQuery, headers=headers)
        commit_info = sha_result.json()
        changed_files = [[x['additions'], x['deletions'], x['changes'], x['filename']] for x in commit_info['files']]
        for file in changed_files:
            with open(newcsv, 'a', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, delimiter=',')
                writer.writerow(thecommit+list(file))

def makeEdgetable(newcsv):
    committer_ms_feature = ['msin', 'msout', '#committer']
    with open(newcsv, 'a', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(committer_ms_feature)
    commit_df = pd.read_csv('mscommits2.csv')
    mslist = list(set(commit_df.loc[:, 'microservice'].values.tolist()))
    for i in range(len(mslist)):
        for j in range(i+1, len(mslist)):
            print([i,j])
            ithemsdf = commit_df.loc[commit_df['microservice'] == mslist[i], :]
            jthemsdf = commit_df.loc[commit_df['microservice'] == mslist[j], :]
            committers_i = list(set(ithemsdf.loc[:, 'committer_name'].values.tolist()))
            authors_i = list(set(ithemsdf.loc[:, 'author_name'].values.tolist()))
            users_i = list(set(committers_i + authors_i))
            committers_j = list(set(jthemsdf.loc[:, 'committer_name'].values.tolist()))
            authors_j = list(set(jthemsdf.loc[:, 'author_name'].values.tolist()))
            users_j = list(set(committers_j + authors_j))
            user_common = [x for x in users_i if x in users_j]
            with open(newcsv, 'a', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, delimiter=',')
                writer.writerow([mslist[i], mslist[j], len(user_common)])

def readProphetJson(jsonfile):
    with open(jsonfile) as json_file:
        data = json.load(json_file)
    with open('prophetedges.csv', 'a', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['Source','Target','Verb'])
    keys = list(data.keys())
    for sourcekey in keys:
        httpverbkeys = data[sourcekey]['DependsOn'].keys()
        for httpverbkey in httpverbkeys:
            print(httpverbkey)
            theVerb = httpverbkey.split(': ')[-1]
            mskeys = data[sourcekey]['DependsOn'][httpverbkey].keys()
            for targetkey in mskeys:
                with open('prophetedges.csv', 'a', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',')
                    writer.writerow([sourcekey,targetkey,theVerb])
        httpverbkeys = data[sourcekey]['Dependants'].keys()
        for httpverbkey in httpverbkeys:
            print(httpverbkey)
            theVerb = httpverbkey.split(': ')[-1]
            mskeys = data[sourcekey]['Dependants'][httpverbkey].keys()
            for targetkey in mskeys:
                with open('prophetedges.csv', 'a', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile, delimiter=',')
                    writer.writerow([targetkey, sourcekey, theVerb])


if __name__ == '__main__':
    token = os.getenv('GITHUB_TOKEN')
    headers = {'Authorization': f'token {token}'}

    desired_width=640
    pd.set_option('display.width', desired_width)
    np.set_printoptions(linewidth=desired_width)
    pd.set_option('display.max_columns', 25)


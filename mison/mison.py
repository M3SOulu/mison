from pydriller import Repository, Commit, ModifiedFile
import pandas as pd


def mine_commits():
    data = []
    for commit in Repository("/Users/abakhtin22/Documents/train-ticket", only_in_branch='master').traverse_commits():
        for file in commit.modified_files:
            data.append([commit.hash, commit.author.name, commit.author.email, commit.committer.name,
                         commit.committer.email, commit.committer_date, file.added_lines, file.deleted_lines,
                         file.new_path])

    data = pd.DataFrame(data, columns=['commit_hash', 'author_name', 'author_email', 'committer_name', 'committer_email',
                                       'commit_date', 'additions', 'deletions', 'filename'])
    print(data)
    data.to_csv('2024.csv', index=False)


if __name__ == '__main__':
    mine_commits()
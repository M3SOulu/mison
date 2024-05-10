from .mison import import_microservice_mapping, mine_commits, construct_network

import pandas

import argparse
import datetime


def main_commit(args):
    microservice_mapping = import_microservice_mapping(args.import_mapping)
    pydriller_kwargs = {'since': args.since,
                        'from_commit': args.from_commit,
                        'from_tag': args.from_tag,
                        'to': args.to,
                        'to_commit': args.to_commit,
                        'to_tag': args.to_tag,
                        'order': args.order,
                        'only_in_branch': args.only_in_branch,
                        'only_no_merge': args.only_no_merge,
                        'only_authors': args.only_authors,
                        'only_commits': args.only_commits,
                        'only_releases': args.only_releases,
                        'filepath': args.filepath,
                        'only_modifications_with_file_types': args.only_modifications_with_file_types
                        }
    data = mine_commits(repo=args.repo, output=args.commit_table, mapping=microservice_mapping, **pydriller_kwargs)
    return data


def main_network(args):
    data = pandas.read_csv(args.commit_table)
    construct_network(data, args.field, output=args.network_output, skip_zero=args.skip_zero)


def main_all(args):
    data = main_commit(args)
    construct_network(data, args.field, output=args.network_output, skip_zero=args.skip_zero)


# Main parser
parser = argparse.ArgumentParser(description='MiSON - MicroService Organisational Network miner')

# Common commit parameters
commit = argparse.ArgumentParser(description='Mine commits of a repository with PyDriller', add_help=False)
commit.add_argument('--repo', type=str, required=True, help='Path to the repository (local path or URL)')
commit.add_argument('--import_mapping', type=str, required=False,
                    help='Python file to import a microservice_mapping function from')
backend = commit.add_argument_group('Backends', 'Available backends for commit mining')
backend = backend.add_mutually_exclusive_group(required=True)
backend.add_argument('--pydriller', action='store_true', help='Use the pydriller library for mining commits'
                                                              'Requires the dependency to be installed')
backend.add_argument('--github', action='store_true', help='Use GitHub API to mine commits of a hosted repository.'
                                                           'Requires an API key')

# Filters for PyDriller
pydriller = commit.add_argument_group('PyDriller backend parameters', 'PyDriller filters for Repository class')
# FROM filters
from_f = pydriller.add_mutually_exclusive_group(required=False)
from_f.add_argument('--since', required=False, type=datetime.datetime.fromisoformat,
                    help='Only commits after this date will be analyzed (converted to datetime object)')
from_f.add_argument('--from_commit', required=False, type=str,
                    help='Only commits after this commit hash will be analyzed')
from_f.add_argument('--from_tag', required=False, type=str, help='Only commits after this commit tag will be analyzed')
# TO filters
to_f = pydriller.add_mutually_exclusive_group(required=False)
to_f.add_argument('--to', required=False, type=datetime.datetime.fromisoformat,
                  help='Only commits up to this date will be analyzed (converted to datetime object)')
to_f.add_argument('--to_commit', required=False, type=str, help='Only commits up to this commit hash will be analyzed')
to_f.add_argument('--to_tag', required=False, type=str, help='Only commits up to this commit tag will be analyzed')
pydriller.add_argument('--order', required=False, choices=['date-order', 'author-date-order', 'topo-order', 'reverse'])
pydriller.add_argument('--only_in_branch', required=False, type=str,
                       help='Only analyses commits that belong to this branch')
pydriller.add_argument('--only_no_merge', required=False, action='store_true',
                       help='Only analyses commits that are not merge commits')
pydriller.add_argument('--only_authors', required=False, nargs='*',
                       help='Only analyses commits that are made by these authors')
pydriller.add_argument('--only_commits', required=False, nargs='*', help='Only these commits will be analyzed')
pydriller.add_argument('--only_releases', required=False, action='store_true',
                       help='Only commits that are tagged (“release” is a term of GitHub, does not actually exist in Git)')
pydriller.add_argument('--filepath', required=False, type=str,
                       help='Only commits that modified this file will be analyzed')
pydriller.add_argument('--only_modifications_with_file_types', required=False, nargs='*',
                       help='Only analyses commits in which at least one modification was done in that file type')

# Parameters for GitHub API
github = commit.add_argument_group('GitHub backend parameters', 'Parameters for mining commits with GitHub backend')
github.add_argument('--github_token', default=None, required=False, help='GitHub API token for mining data.'
                                                                         'Can also be provided as env. GITHUB_TOKEN')
github.add_argument('--per_page', type=int, default=100, help='How many commits per page request from GitHub API')

# Common network parameters
network = argparse.ArgumentParser(description='Construct a developer network from a commit table', add_help=False)
network.add_argument('--field', choices=['file', 'service'], required=True,
                     help='Which field to use for network weight')
network.add_argument('--network_output', type=str, required=False, help='Output path for network')
network.add_argument('--skip_zero', action='store_true', help='If set, do no write rows containing zero weight')

# Sub-commands for main
subparsers = parser.add_subparsers(required=True)

# Commit command
commit_sub = subparsers.add_parser('commit', parents=[commit], help='Mine commits of a repository with PyDriller',
                                   conflict_handler='resolve')
commit_sub.add_argument('--commit_table', type=str, required=True,
                        help='Output path for the csv table of mined commits')
commit_sub.set_defaults(func=main_commit)

# Network command
network_sub = subparsers.add_parser('network', parents=[network],
                                    help='Construct a developer network from a commit table',
                                    conflict_handler='resolve')
# Network only needs input file if called separately
network_sub.add_argument('--commit_table', type=str, required=True, help='Input path of the csv table of mined commits')
network_sub.set_defaults(func=main_network)

# End-to-end command
end_to_end = subparsers.add_parser('all', parents=[commit, network],
                                   help='End-to-end network generation from the repository', conflict_handler='resolve')
end_to_end.add_argument('--commit_table', type=str, required=False,
                        help='Output path for the csv table of mined commits')
end_to_end.set_defaults(func=main_all)

# Parse the arguments
args = parser.parse_args()
args.func(args)

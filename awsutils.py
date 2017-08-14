import boto3
import sys
from subprocess import check_output


def run_cli_cmd(cmd):
    return check_output(cmd.split(), shell=True) if sys.platform.startswith('win') else check_output(cmd.split())


def get_clients(profile_name):
    session = boto3.Session(profile_name=profile_name)
    return [session.client('s3'), session.client('emr')]


def get_emr_cluster_with_name(emrclient, cluster_name):
    running_clusters = emrclient.list_clusters(
        ClusterStates=[
            'STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'
        ]
    )
    same_name_clusters = \
        [{'id': c['Id'], 'name': c['Name'], 'state': c['Status']['State']} for c in running_clusters['Clusters']
         if c['Name'] == cluster_name]
    return same_name_clusters


def tokenize_emr_step_args(arguments):
    return '[{}]'.format(','.join(arguments.split()))

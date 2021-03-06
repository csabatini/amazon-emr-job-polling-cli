from __future__ import print_function
import json
import logging
import os
import sys
from os.path import dirname, join
from subprocess import check_output

import boto3
import yaml
from jinja2 import Template

valid_runtimes = ['scala', 'java', 'python']


class AWSApi(object):
    def __init__(self, profile=None):
        self.profile = profile
        self.session = \
            boto3.Session(profile_name=profile) if profile else boto3.Session()
        self.s3 = self.session.client('s3')
        self.emr = self.session.client('emr')

    def get_emr_cluster_with_name(self, cluster_name):
        active_clusters = self.emr.list_clusters(
            ClusterStates=[
                'STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'
            ]
        )
        same_name_clusters = \
            [{'id': c['Id'], 'name': c['Name'], 'state': c['Status']['State']}
             for c in active_clusters['Clusters'] if c['Name'] == cluster_name]
        return same_name_clusters

    def is_cluster_active(self, cluster_name):
        return len(self.get_emr_cluster_with_name(cluster_name)) == 1

    def list_running_cluster_instances(self, cluster_id):
        return self.emr.list_instances(
            ClusterId=cluster_id, InstanceStates=['RUNNING'])

    def list_cluster_steps(self, cluster_id, job_name, active_only=False):

        if active_only:
            states = ['PENDING', 'RUNNING']
        else:
            states = [
                'PENDING',
                'CANCEL_PENDING',
                'RUNNING',
                'COMPLETED',
                'CANCELLED',
                'FAILED',
                'INTERRUPTED']

        active_jobs = self.emr.list_steps(
            ClusterId=cluster_id,
            StepStates=states
        )
        return [s for s in active_jobs['Steps'] if s['Name'] == job_name]

    def terminate_clusters(self, cluster_name, config):
        clusters = self.get_emr_cluster_with_name(cluster_name)

        terminate_template = \
            Template(('aws emr{% if profile %} --profile {{ profile }}'
                      '{% endif %} terminate-clusters --cluster-id '
                      '{{ clust_id }}'))

        for cluster_info in clusters:
            config['clust_id'] = cluster_info['id']
            print('Terminating cluster: {}\n'.format(json.dumps(cluster_info)))
            term_command = terminate_template.render(config)
            print('\n{}\n'.format(term_command))
            run_shell_command(term_command)


def run_shell_command(cmd):
    return check_output(cmd.split()) if not sys.platform.startswith('win') \
        else check_output(cmd.split(), shell=True)


def tokenize_emr_step_args(arguments):
    return '[{}]'.format(','.join(arguments.split()))


def load_config(file_name, env_key):
    """Utility function to parse a YAML configuration file"""
    default_log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=default_log_fmt)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)

    module_dir = dirname(__file__)
    file_path = os.getenv(env_key, None) or join(module_dir, file_name)

    try:
        logger.info('Loading configuration from file: {}'.format(file_path))
        with open(file_path, 'r') as f:
            return yaml.safe_load(f.read())
    except (OSError, IOError) as e:
        logger.exception('Failed to read file: {}'.format(file_path), e)
        return dict()


def log_assertion(condition, log_msg, description):
    try:
        if not condition:
            raise ValueError(description)
        logging.info(log_msg)
    except ValueError as e:
        logging.exception("exception={}, {}".format(type(e).__name__, log_msg))
        raise e


def try_response_json_lookup(response, key):
    try:
        return response.json()[key]
    except AttributeError:
        return 'none'
    except ValueError:
        return 'none'

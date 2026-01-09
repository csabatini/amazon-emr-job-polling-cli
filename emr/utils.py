import sys
import json
import logging
import boto3
from jinja2 import Template
from subprocess import check_output

profiles = {
    'qa': 'saml3',
    'pre-prod': 'saml4',
    'prod': 'saml5'
}


class AWSApi(object):
    def __init__(self, profile=None):
        self.profile = profile
        self.session = boto3.Session(profile_name=profile)
        self.s3 = self.session.client('s3')
        self.emr = self.session.client('emr')

    def get_emr_cluster_with_name(self, cluster_name):
        running_clusters = self.emr.list_clusters(
            ClusterStates=[
                'STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'
            ]
        )
        same_name_clusters = \
            [{'id': c['Id'], 'name': c['Name'], 'state': c['Status']['State']} for c in running_clusters['Clusters']
             if c['Name'] == cluster_name]
        return same_name_clusters

    def terminate_clusters(self, cluster_name, config):
        clusters = self.get_emr_cluster_with_name(cluster_name)
        terminate_template = Template('aws emr{% if not airflow %} --profile {{ profile }}{% endif %} '
                                      'terminate-clusters --cluster-id {{ clust_id }}')

        for cluster_details in clusters:
            config['clust_id'] = cluster_details['id']
            logging.info('\n\nTerminating cluster: {}\n\n'.format(json.dumps(cluster_details)))
            term_command = terminate_template.render(config)
            logging.info(term_command)
            run_cli_cmd(term_command)

    def get_remote_state_values(self, env, s3_key, output_keys):
        obj = self.s3.get_object(Bucket='{}-sonic-terraform-state-files'.format(env), Key='{}-{}'.format(env, s3_key))

        state = json.loads(obj['Body'].read())
        outputs = state['modules'][0]['outputs']
        result = {}
        for k in output_keys:
            if k in outputs:
                result[k] = outputs[k]['value']
        return result


def get_aws_profile(env, airflow, cicd):
    if airflow:
        return None
    elif cicd:
        return 'bdp-{}'.format(env.replace('-', ''))
    else:  # use saml profiles from centrify tool
        return profiles[env]


def run_cli_cmd(cmd):
    return check_output(cmd.split(), shell=True) if sys.platform.startswith('win') else check_output(cmd.split())


def get_artifact_parts(artifact_path):
    return None if not artifact_path else artifact_path.replace('s3://', '').split('/', 1)


def log_assertion(condition, log_msg, description):
    try:
        assert condition, description
        logging.info(log_msg)
    except AssertionError, e:
        logging.exception("exception={}, {}".format(type(e).__name__, log_msg))
        raise e


def tokenize_emr_step_args(arguments):
    return '[{}]'.format(','.join(arguments.split()))

import sys
import json
import logging
import boto3
from jinja2 import Template
from subprocess import check_output

valid_runtimes = ['scala', 'python']


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

    def list_running_cluster_instances(self, cluster_id):
        return self.emr.list_instances(ClusterId=cluster_id, InstanceStates=['RUNNING'])

    def list_cluster_steps(self, cluster_id, job_name, active_only=False):

        if active_only:
            states = ['PENDING', 'RUNNING']
        else:
            states = ['PENDING', 'CANCEL_PENDING', 'RUNNING', 'COMPLETED', 'CANCELLED', 'FAILED', 'INTERRUPTED']

        active_jobs = self.emr.list_steps(
            ClusterId=cluster_id,
            StepStates=states
        )
        return [s for s in active_jobs['Steps'] if s['Name'] == job_name]

    def put_job_kill_marker(self, bucket, job_name):
        key = '{}.kill.txt'.format(job_name)
        logging.info("awstype=s3, action=put-kill-marker, status=started, bucket={}, key={}".format(bucket, key))
        self.s3.put_object(Bucket=bucket, Body='', Key=key, ServerSideEncryption='AES256')
        logging.info("awstype=s3, action=put-kill-marker, status=ended, bucket={}, key={}".format(bucket, key))

    def delete_job_kill_marker(self, bucket, job_name):
        key = '{}.kill.txt'.format(job_name)
        response = self.s3.list_objects_v2(
            Bucket=bucket,
            Prefix=key
        )

        if response['KeyCount'] != 1:  # no kill marker to remove
            return

        logging.info("awstype=s3, action=delete-kill-marker, status=started, bucket={}, key={}".format(bucket, key))
        self.s3.delete_object(Bucket=bucket, Key=key)
        logging.info("awstype=s3, action=delete-kill-marker, status=ended, bucket={}, key={}".format(bucket, key))

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
    else:  # use environment
        return env


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


def try_response_json_lookup(response, key):
    try:
        return response.json()[key]
    except ValueError, e:
        return 'none'


def tokenize_emr_step_args(arguments):
    return '[{}]'.format(','.join(arguments.split()))

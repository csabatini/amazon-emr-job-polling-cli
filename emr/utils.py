import json
import logging
import os
import sys
from os.path import dirname, join
from subprocess import check_output

import yaml
from boto3 import Session
from jinja2 import Template
from emr.templates import add_checkpoint_cp_step_template

valid_runtimes = ['scala', 'java', 'python']


class AWSApi(object):
    def __init__(self, profile=None):
        self.profile = profile
        self.session = \
            Session(profile_name=profile) if profile else Session()
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

    def has_s3_checkpoints(self, env, bucket, job):
        numObjects = self.s3.list_objects_v2(
            Bucket=bucket,
            Prefix=job + '/'
        )['KeyCount']
        result = numObjects > 1
        # don't count single folder metadata object
        logging.info('environment={0}, job={1}, action='
                     'check-job-has-checkpoints, bucket={2}, numObjects={3}, '
                     'result={4}'.format(env, job, bucket, numObjects, result))
        return result

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

    def add_checkpoint_copy_job(self, config, job_name):
        cli_cmd = add_checkpoint_cp_step_template.render(config)
        logging.info('\n\n{}\n\n'.format(cli_cmd))

        output = run_cli_cmd(cli_cmd)
        logging.info(output)
        log_msg = "environment={}, cluster={}, job={}, action=add-job-step" \
            .format(config['env'], config['cluster_name'], job_name)
        log_assertion('StepIds' in output, log_msg,
                      'StepIds not found in console output')

    def put_job_shutdown_marker(self, bucket, job_name):
        key = '{}.shutdown.txt'.format(job_name)
        logging.info('awstype=s3, action=put-shutdown-marker, status=started, '
                     'bucket={}, key={}'.format(bucket, key))
        self.s3.put_object(
            Bucket=bucket,
            Body='',
            Key=key,
            ServerSideEncryption='AES256')
        logging.info('awstype=s3, action=put-shutdown-marker, status=ended, '
                     'bucket={}, key={}'.format(bucket, key))

    def delete_job_shutdown_marker(self, bucket, job_name):
        key = '{}.shutdown.txt'.format(job_name)
        response = self.s3.list_objects_v2(
            Bucket=bucket,
            Prefix=key
        )

        if response['KeyCount'] != 1:  # no kill marker to remove
            return

        logging.info('awstype=s3, action=delete-shutdown-marker, '
                     'status=started, bucket={}, key={}'.format(bucket, key))
        self.s3.delete_object(Bucket=bucket, Key=key)
        logging.info('awstype=s3, action=delete-shutdown-marker, '
                     'status=ended, bucket={}, key={}'.format(bucket, key))

    def terminate_clusters(self, cluster_name, config):
        clusters = self.get_emr_cluster_with_name(cluster_name)

        terminate_template = Template(
            'aws emr{% if profile %} --profile {{ profile }}{% endif %} '
            'terminate-clusters --cluster-id {{ clust_id }}')

        for cluster_details in clusters:
            config['clust_id'] = cluster_details['id']
            logging.info(
                '\nTerminating cluster: {}\n'.format(
                    json.dumps(cluster_details)))
            term_command = terminate_template.render(config)
            logging.info('\n{}\n'.format(term_command))
            run_cli_cmd(term_command)

    def get_remote_state_values(self, env, s3_key, output_keys):
        obj = self.s3.get_object(
            Bucket='{}-sonic-terraform-state-files'.format(env),
            Key='{}-{}'.format(env, s3_key))

        state = json.loads(obj['Body'].read())
        outputs = state['modules'][0]['outputs']
        result = {}
        for k in output_keys:
            if k in outputs:
                result[k] = outputs[k]['value']
        return result


def run_cli_cmd(cmd):
    return check_output(cmd.split()) if not sys.platform.startswith('win') \
        else check_output(cmd.split(), shell=True)


def tokenize_emr_step_args(arguments):
    return '[{}]'.format(','.join(arguments.split()))


def get_artifact_parts(artifact_path):
    return None if not artifact_path \
        else artifact_path.replace('s3://', '').split('/', 1)


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

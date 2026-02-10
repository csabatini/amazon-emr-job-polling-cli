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


class AWSApi(object):
    """AWS API client wrapper for EMR and S3 operations.

    Provides a simplified interface for common AWS EMR cluster operations
    including cluster discovery, step management, and termination.

    Attributes:
        profile (str): AWS profile name for authentication.
        session (boto3.Session): Boto3 session instance.
        s3 (boto3.client): S3 client instance.
        emr (boto3.client): EMR client instance.

    Example:
        >>> api = AWSApi(profile='my-profile')
        >>> clusters = api.get_emr_cluster_with_name('my-cluster')
    """
    def __init__(self, profile=None):
        self.profile = profile
        self.session = \
            boto3.Session(profile_name=profile) if profile else boto3.Session()
        self.s3 = self.session.client('s3')
        self.emr = self.session.client('emr')

    def get_emr_cluster_with_name(self, cluster_name):
        """Get all active EMR clusters with the specified name.

        Args:
            cluster_name (str): The name of the EMR cluster to search for.

        Returns:
            list: A list of dictionaries containing cluster information.
                Each dictionary has keys: 'id', 'name', 'state'.

        Note:
            Only searches clusters in active states:
            STARTING, BOOTSTRAPPING, RUNNING, WAITING.
        """
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
        """Check if exactly one active cluster exists with the given name.

        Args:
            cluster_name (str): The name of the EMR cluster to check.

        Returns:
            bool: True if exactly one active cluster is found, False otherwise.
        """
        return len(self.get_emr_cluster_with_name(cluster_name)) == 1

    def list_running_cluster_instances(self, cluster_id):
        """List all running instances for a given EMR cluster.

        Args:
            cluster_id (str): The ID of the EMR cluster.

        Returns:
            dict: Response from EMR list_instances API containing
                running instances.
        """
        return self.emr.list_instances(
            ClusterId=cluster_id, InstanceStates=['RUNNING'])

    def list_cluster_steps(self, cluster_id, job_name, active_only=False):
        """List EMR cluster steps filtered by job name and state.

        Args:
            cluster_id (str): The ID of the EMR cluster.
            job_name (str): The name of the job/step to filter by.
            active_only (bool): If True, only return PENDING and RUNNING
                steps. If False, return all step states. Defaults to False.

        Returns:
            list: A list of step dictionaries matching the job name.
        """
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
        """Terminate all active EMR clusters with the specified name.

        Args:
            cluster_name (str): The name of the EMR clusters to terminate.
            config (dict): Configuration dictionary containing 'profile' key
                          for AWS CLI authentication.

        Note:
            Prints cluster information and termination commands to stdout.
            Uses AWS CLI to execute termination commands.
        """
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
    """Execute a shell command and return its output.

    Args:
        cmd (str): The shell command to execute.

    Returns:
        bytes: The command output as bytes.

    Note:
        On Windows, executes with shell=True. On other platforms,
        executes directly.
    """
    return check_output(cmd.split()) if not sys.platform.startswith('win') \
        else check_output(cmd.split(), shell=True)


def tokenize_emr_step_args(arguments):
    """Format space-separated arguments as a comma-separated list in brackets.

    Args:
        arguments: Space-separated string of arguments.

    Returns:
        str: Formatted string with arguments comma-separated
            and wrapped in brackets.

    Example:
        >>> tokenize_emr_step_args("arg1 arg2 arg3")
        "[arg1,arg2,arg3]"
    """
    return '[{}]'.format(','.join(arguments.split()))


def load_config(file_name, env_key):
    """Load a YAML configuration file.

    Args:
        file_name: Name of the configuration file to load.
        env_key: Environment variable key to check for custom file path.

    Returns:
        dict: Parsed YAML configuration, or empty dict if file cannot be read.

    Note:
        Sets up logging configuration as a side effect.
    """
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
        logger.exception('Failed to read file: {}'.format(file_path))
        return dict()


def log_assertion(condition, log_msg, description):
    """Log an assertion and raise ValueError if condition is False.

    Args:
        condition: Boolean condition to evaluate.
        log_msg: Message to log on success or include in error log.
        description: Error message to raise if condition is False.

    Raises:
        ValueError: If condition evaluates to False,
            with the provided description.
    """
    try:
        if not condition:
            raise ValueError(description)
        logging.info(log_msg)
    except ValueError as e:
        logging.exception("exception={}, {}".format(type(e).__name__, log_msg))
        raise e


def try_response_json_lookup(response, key):
    """Safely extract a value from a JSON response by key.

    Args:
        response: A response object with a .json() method
            (e.g., requests.Response).
        key: The key to look up in the JSON response.

    Returns:
        The value associated with the key if found, otherwise returns 'none'.

    Raises:
        None. All exceptions (AttributeError, ValueError) are caught
            and return 'none'.
    """
    try:
        return response.json()[key]
    except AttributeError:
        return 'none'
    except ValueError:
        return 'none'

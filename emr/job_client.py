from __future__ import print_function
import click
import datetime
import json
import logging
import pytz
import time
import collections
import logging.config
import six
from functools import reduce

import emr.utils
from emr.templates import spark_template, add_spark_step_template
from emr.utils import valid_runtimes


@click.command()
@click.pass_context
@click.option('--env',
              help='Environment to deploy to (required).',
              default='')
@click.option('--profile',
              default='',
              help='Optional AWS profile credentials to be used.')
@click.option('--job-name',
              help='Name for the EMR Step & Spark job.',
              required=True)
@click.option('--job-runtime', default='scala',
              help='Runtime for Spark; should be Scala, Java, or Python.')
@click.option('--job-timeout',
              default=60,
              help='Spark job timeout in minutes.')
@click.option('--cluster-name',
              help='Name for the EMR cluster.',
              required=True)
@click.option('--artifact-path',
              help='Amazon S3 path to the Spark artifact.')
@click.option('--poll-cluster',
              is_flag=True,
              help='Option to poll for the final job state.')
@click.option('--auto-terminate',
              'terminate',
              flag_value=True,
              help='Terminate the cluster after the Spark job finishes.')
@click.option('--no-auto-terminate',
              'terminate',
              flag_value=False,
              default=False,
              help='Keep the cluster up after the job finishes (default).')
@click.option('--dryrun',
              is_flag=True,
              help='Output the EMR command without executing it.')
@click.option('--job-args',
              default='',
              help='Extra arguments for the Spark application.')
@click.option('--job-configs',
              default='',
              help='Extra configs for the Spark application.')
@click.option('--main-class', help='Main class of the Spark application.')
def parse_arguments(context, env, profile, job_name, job_runtime, job_timeout,
                    cluster_name, artifact_path, poll_cluster, terminate,
                    dryrun, job_args, job_configs, main_class):
    handle_job_request(context.params)


def handle_job_request(params):
    config = collections.OrderedDict(sorted(params.items()))
    extract_keys = [
        'env',
        'profile',
        'job_name',
        'job_runtime',
        'job_timeout',
        'cluster_name',
        'artifact_path',
        'poll_cluster',
        'terminate',
        'dryrun']

    artifact_path, cluster_name, dryrun, env, job_name, job_runtime, \
        job_timeout, poll_cluster, profile, terminate = \
        [v for k, v in six.iteritems(config) if k in extract_keys]

    log_msg = ('environment={}, cluster={}, job={}, action=check-runtime, '
               'runtime={}'.format(env, cluster_name, job_name, job_runtime))
    emr.utils.log_assertion(
        job_runtime.lower() in valid_runtimes, log_msg,
        '--job-runtime should be in {}'.format(valid_runtimes))

    aws_api = emr.utils.AWSApi(profile) if profile else emr.utils.AWSApi()

    # get existing cluster info
    clust_info = aws_api.get_emr_cluster_with_name(cluster_name)
    log_msg = (
        'environment={}, cluster={}, job={}, action=get-clusters, '
        'count={}, clusterList={}'.format(
            env,
            cluster_name,
            job_name,
            len(clust_info),
            json.dumps(clust_info)))
    emr.utils.log_assertion(
        len(clust_info) == 1,
        log_msg,
        'Expected 1 but found {} running clusters with name {}'.format(
            len(clust_info),
            cluster_name))

    # add cluster id to the config
    cluster_id, config['cluster_id'] = clust_info[0]['id'], clust_info[0]['id']

    cli_cmd = ''
    # submit a new EMR Step to the running cluster
    if artifact_path:
        config['step_args'] = \
            emr.utils.tokenize_emr_step_args(spark_template.render(config))

        cli_cmd = add_spark_step_template.render(config)
        logging.info(cli_cmd)
        if not dryrun:
            output = emr.utils.run_shell_command(cli_cmd)
            print(output)
            log_msg = ('environment={}, cluster={}, job={}, action='
                       'add-job-step'.format(env, cluster_name, job_name))
            emr.utils.log_assertion('StepIds' in output, log_msg,
                                    'StepIds not found in terminal output')

    # monitor state of the EMR Step (Spark Job)
    if poll_cluster:
        job_state = 'UNKNOWN'

        while job_state != 'COMPLETED':
            time.sleep(60)
            jobs = aws_api.list_cluster_steps(
                cluster_id, job_name, active_only=False)
            if job_state == 'UNKNOWN':
                log_msg = (
                    'environment={}, cluster={}, job={}, '
                    'action=get-steps, clusterId={}, numSteps={}'.format(
                        env, cluster_name, job_name, cluster_id, len(jobs)))
                emr.utils.log_assertion(
                    len(jobs) > 0, log_msg,
                    'Expected 1+ but found {} jobs for name {}'.format(
                        len(jobs),
                        job_name))
            current_job = reduce(
                (lambda j1, j2: j1
                    if j1['Status']['Timeline']['CreationDateTime'] >
                    j2['Status']['Timeline']['CreationDateTime'] else j2),
                jobs)
            job_metrics = cluster_step_metrics(current_job)
            logging.info(
                'environment={}, cluster={}, job={}, action=poll-cluster, '
                'stepId={}, state={}, createdTime={}, minutesElapsed={}'
                .format(
                    env, cluster_name, job_name, job_metrics['id'],
                    job_metrics['state'],
                    job_metrics['createdTime'],
                    job_metrics['minutesElapsed']))
            job_state = job_metrics['state']
            minutes_elapsed = job_metrics['minutesElapsed']

            # check for termination events: failure or timeout exceeded
            if job_metrics['state'] == 'FAILED':
                log_msg = (
                    'environment={}, cluster={}, job={}, '
                    'action=exit-failed-state, stepId={}, state={}'.format(
                        env,
                        cluster_name,
                        job_name,
                        job_metrics['id'],
                        job_metrics['state']))
                emr.utils.log_assertion(
                    job_metrics['state'] != 'FAILED',
                    log_msg,
                    'Job in invalid state {}'.format(job_metrics['state']))

            elif job_timeout is not None and minutes_elapsed > job_timeout:
                log_msg = \
                    ('environment={}, cluster={}, job={}, action='
                     'exceeded-timeout, minutes={}'.format(env,
                                                           cluster_name,
                                                           job_name,
                                                           job_timeout))
                emr.utils.log_assertion(
                    minutes_elapsed <= job_timeout, log_msg,
                    'Job exceeded timeout {}'.format(job_timeout))

        if terminate:
            aws_api.terminate_clusters(cluster_name, config)
    return cli_cmd


def cluster_step_metrics(step_info):
    created_dt = step_info['Status']['Timeline']['CreationDateTime']
    str_created_dt = created_dt.strftime("%Y-%m-%dT%H-%M-%S")
    seconds_elapsed = \
        (datetime.datetime.now(pytz.utc) - created_dt).total_seconds()
    minutes_elapsed = divmod(seconds_elapsed, 60)[0]
    return {
        'id': step_info['Id'],
        'name': step_info['Name'],
        'state': step_info['Status']['State'],
        'createdTime': str_created_dt,
        'minutesElapsed': minutes_elapsed
    }


if __name__ == '__main__':
    log_config = emr.utils.load_config('logging.yml', 'LOG_CFG')
    logging.config.dictConfig(log_config)
    parse_arguments()

from __future__ import print_function
import click
import datetime
import json
import logging
import pytz
import sys
import time
import grequests
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
              prompt='Enter the environment to deploy to (qa/pre-prod/prod)',
              help='Environment to deploy to (required).',
              required=True)
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
@click.option('--job-mode',
              type=click.Choice(['batch', 'streaming']),
              required=True,
              help='Run mode of the Spark job, must be batch or streaming.')
@click.option('--cluster-name',
              default='DataPipeline',
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
              default=True,
              help='Keep the cluster up after the job finishes (default).')
@click.option('--checkpoint-bucket',
              default='',
              help='S3 bucket that persists Spark streaming checkpoints.')
@click.option('--shutdown',
              is_flag=True,
              help='Flag to shutdown the Spark streaming job gracefully')
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
                    job_mode, cluster_name, artifact_path, poll_cluster,
                    terminate, checkpoint_bucket, shutdown, dryrun, job_args,
                    job_configs, main_class):
    handle_job_request(context.params)


def handle_job_request(params):
    config = collections.OrderedDict(sorted(params.items()))
    extract_keys = [
        'env',
        'profile',
        'job_mode',
        'job_name',
        'job_runtime',
        'job_timeout',
        'cluster_name',
        'artifact_path',
        'poll_cluster',
        'terminate',
        'checkpoint_bucket',
        'shutdown',
        'dryrun']

    artifact_path, checkpoint_bucket, cluster_name, dryrun, env, job_mode, \
        job_name, job_runtime, job_timeout, poll_cluster, profile, shutdown, \
        terminate = [v for k, v in six.iteritems(config) if k in extract_keys]

    log_msg = ('environment={}, cluster={}, job={}, action=check-runtime, '
               'runtime={}'.format(env, cluster_name, job_name, job_runtime))
    emr.utils.log_assertion(
        job_runtime.lower() in valid_runtimes, log_msg,
        '--job-runtime should be in {}'.format(valid_runtimes))

    if not checkpoint_bucket:
        checkpoint_bucket = '{}-checkpoints'.format(env)
    config['checkpoint_bucket'] = checkpoint_bucket
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
    cluster_job_ids = [job_name]

    if shutdown:
        poll_cluster = True if not dryrun else False
        job_timeout = None
        # put marker file in s3 to initiate streaming job shutdown
        shutdown_initiated = shutdown_streaming_job(
            aws_api, config, job_name, checkpoint_bucket)
        # when job isn't running, remove it from the list of jobs to poll
        if not shutdown_initiated:
            cluster_job_ids = []

        # add job to copy the spark checkpoints files to s3
        copy_job_id = 'S3DistCp'
        aws_api.add_checkpoint_copy_job(config, copy_job_id)
        cluster_job_ids.append(copy_job_id)

    # submit a new EMR Step to the running cluster
    if artifact_path and not shutdown:
        config['artifact_parts'] = emr.utils.get_artifact_parts(artifact_path)
        config['step_args'] = \
            emr.utils.tokenize_emr_step_args(spark_template.render(config))

        distribute_dependencies(aws_api, cluster_id, cluster_name, config, env,
                                job_name, job_runtime)
        cli_cmd = add_spark_step_template.render(config)
        logging.info('\n\n{}'.format(cli_cmd))
        if not dryrun:
            output = emr.utils.run_shell_command(cli_cmd)
            print(output)
            log_msg = ('environment={}, cluster={}, job={}, action='
                       'add-job-step'.format(env, cluster_name, job_name))
            emr.utils.log_assertion('StepIds' in output, log_msg,
                                    'StepIds not found in console output')

    # monitor state of the EMR Step (Spark Job)
    if poll_cluster:
        for job_id in cluster_job_ids:
            job_state = 'UNKNOWN'

            while job_state != 'COMPLETED':
                time.sleep(60)
                jobs = aws_api.list_cluster_steps(
                    cluster_id, job_id, active_only=False)
                if job_state == 'UNKNOWN':
                    log_msg = (
                        'environment={}, cluster={}, job={}, '
                        'action=get-steps, clusterId={}, numSteps={}'.format(
                            env, cluster_name, job_id, cluster_id, len(jobs)))
                    emr.utils.log_assertion(
                        len(jobs) > 0, log_msg,
                        'Expected 1+ but found {} jobs for name {}'.format(
                            len(jobs),
                            job_id))
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
                        env, cluster_name, job_id, job_metrics['id'],
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
                            job_id,
                            job_metrics['id'],
                            job_metrics['state']))
                    emr.utils.log_assertion(
                        job_metrics['state'] != 'FAILED',
                        log_msg,
                        'Job in invalid state {}'.format(job_metrics['state']))

                elif minutes_elapsed > job_timeout and job_timeout is not None:
                    log_msg = ('environment={}, cluster={}, job={}, '
                               'action=exceeded-timeout, minutes={}'.format(
                                   env, cluster_name, job_id, job_timeout))
                    if job_mode == 'batch':
                        emr.utils.log_assertion(
                            False, log_msg,
                            'Job exceeded timeout {}'.format(job_timeout))
                    else:
                        logging.info(log_msg)
                        # stream monitoring period elapsed without failing
                        sys.exit(0)

        if shutdown:
            aws_api.delete_job_shutdown_marker(checkpoint_bucket, job_name)
        if terminate:
            aws_api.terminate_clusters(cluster_name, config)
    return cli_cmd


def distribute_dependencies(aws_api, cluster_id, cluster_name, config, env,
                            job_name, job_runtime):
    ec2_instances = \
        aws_api.list_running_cluster_instances(cluster_id)

    ips = [i['PrivateIpAddress'] for i in ec2_instances['Instances']]
    logging.info('environment={}, cluster={}, job={}, action=get-instances'
                 ', count={}, ips={}'.format(env,
                                             cluster_name,
                                             job_name,
                                             len(ips),
                                             json.dumps(ips)))
    artifact_payload = {
        'runtime': job_runtime,
        'bucket': config['artifact_parts'][0],
        'key': config['artifact_parts'][1]
    }
    api_log = 'environment={}, cluster={}, job={}, action={}, bucket={}, '
    'key={}, ip={}, status_code={}, message={}'

    # download artifacts and install dependencies to the EMR nodes by
    # calling the bootstrapped web service
    download_requests = []
    install_requests = []
    for ip in ips:
        r = grequests.post(
            'http://{}:8080/download'.format(ip),
            json=artifact_payload)
        download_requests.append(r)
        if job_runtime.lower() == 'python':
            r = grequests.get('http://{}:8080/requirements'.format(ip))
            install_requests.append(r)

    download_responses = \
        zip(ips, grequests.map(download_requests, size=7))
    validate_responses(download_responses, api_log, config, 'download')

    install_responses = \
        zip(ips, grequests.map(install_requests, size=7))
    validate_responses(install_responses, api_log, config, 'install')


def validate_responses(responses, api_log, config, action):
    status_code_assertion = 'Excepted 200 but found {} HTTP status code'
    for resp in responses:
        log_msg = api_log.format(
            config['env'],
            config['cluster_name'],
            config['job_name'],
            action,
            config['artifact_parts'][0],
            config['artifact_parts'][1],
            resp[0],
            resp[1].status_code,
            emr.utils.try_response_json_lookup(resp[1], 'message'))
        emr.utils.log_assertion(
            resp[1].status_code == 200,
            log_msg,
            status_code_assertion.format(resp[1].status_code))


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


def shutdown_streaming_job(api, configs, job_name, checkpoint_s3_bucket):
    jobs = api.list_cluster_steps(
        configs['cluster_id'],
        job_name, active_only=True)
    count = len(jobs)

    log_msg = ('environment={}, cluster={}, job={}, '
               'action=shutdown, is_currently_active={}')

    if count == 1:
        api.put_job_shutdown_marker(checkpoint_s3_bucket, job_name)
        logging.info(
            log_msg.format(
                configs['env'],
                configs['cluster_name'],
                job_name,
                True))
        return True  # shutdown was initiated with marker file
    else:
        logging.info(
            log_msg.format(
                configs['env'],
                configs['cluster_name'],
                job_name,
                False))
        return False


if __name__ == '__main__':
    log_config = emr.utils.load_config('logging.yml', 'LOG_CFG')
    logging.config.dictConfig(log_config)
    parse_arguments()

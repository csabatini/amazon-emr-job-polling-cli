import click
import json
import logging
import pytz
import sys
import time
import grequests
import copy
import collections
from datetime import datetime

from templates import spark_template, add_spark_step_template
from utils import *


@click.command()
@click.pass_context
@click.option('--env', prompt='Enter the environment to deploy to (qa/pre-prod/prod)',
              help='Environment to deploy to (required).', required=True)
@click.option('--job-name', help='Name for the EMR Step & Spark job.', required=True)
@click.option('--job-runtime', default='scala', help='Runtime for Spark, should be either Scala or Python.')
@click.option('--job-timeout', default=60, help='Spark job timeout in minutes.')
@click.option('--job-mode', type=click.Choice(['batch', 'streaming']), required=True,
              help='Run mode of the Spark job, must be either batch or streaming.')
@click.option('--cluster-name', default='DataPipeline', help='Name for the EMR cluster.')
@click.option('--artifact-path', help='Amazon S3 path to the Spark artifact.')
@click.option('--poll-cluster', is_flag=True, help='Option to poll the cluster for job state (completed/failed).')
@click.option('--auto-terminate', is_flag=True, help='Terminate the cluster after the Spark job finishes.')
@click.option('--checkpoint-bucket', default='', help='S3 bucket used for persisten Spark streaming checkpoints;')
@click.option('--shutdown', is_flag=True, help='Indicator to shutdown the Spark streaming job gracefully')
@click.option('--cicd', is_flag=True, help='Indicator for deployment from gocd; uses IAM profile auth.')
@click.option('--airflow', is_flag=True, help='Indicator for deployment from airflow; uses EC2 instance role auth.')
@click.option('--dryrun', is_flag=True, help='Print out the EMR command without actually running it.')
@click.option('--job-args', default='', help='Extra arguments for the Spark application.')
@click.option('--job-configs', default='', help='Extra configs for the Spark application.')
@click.option('--main-class', help='Main class of the Spark application.')
def parse_arguments(context, env, job_name, job_runtime, job_timeout, job_mode, cluster_name, artifact_path,
                    poll_cluster, auto_terminate, checkpoint_bucket, shutdown, cicd, airflow, dryrun, job_args,
                    job_configs, main_class):
    handle_job_request(context.params, None)


def handle_job_request(params, api):
    config = collections.OrderedDict(sorted(copy.deepcopy(params).items()))
    extract_keys = ['env', 'job_mode', 'job_name', 'job_runtime', 'job_timeout', 'cluster_name', 'artifact_path',
                    'poll_cluster', 'auto_terminate', 'checkpoint_bucket', 'shutdown', 'cicd', 'airflow', 'dryrun']

    airflow, artifact_path, auto_terminate, checkpoint_bucket, cicd, cluster_name, dryrun, env, job_mode, job_name, \
    job_runtime, job_timeout, poll_cluster, shutdown = [v for k, v in config.iteritems() if k in extract_keys]

    log_msg = 'environment={}, cluster={}, job={}, action=check-runtime, runtime={}' \
        .format(env, cluster_name, job_name, job_runtime)
    log_assertion(job_runtime.lower() in valid_runtimes, log_msg, 'runtime should be either Scala or Python')

    # determine aws profile to use for AWSApi
    config['profile'] = get_aws_profile(env, airflow, cicd)
    checkpoint_bucket = checkpoint_bucket if checkpoint_bucket else '{}-checkpoints'.format(env)
    config['checkpoint_bucket'] = checkpoint_bucket
    aws_api = api if api is not None else AWSApi(config['profile'])

    # get existing cluster info
    cluster_info = aws_api.get_emr_cluster_with_name(cluster_name)
    log_msg = "environment={}, cluster={}, job={}, action=get-clusters, count={}, clusterList={}" \
        .format(env, cluster_name, job_name, len(cluster_info), json.dumps(cluster_info))
    log_assertion(len(cluster_info) == 1, log_msg,
                  'Expected 1 but found {} running clusters with name {}'.format(len(cluster_info), cluster_name))

    # add cluster id to the config
    config['cluster_id'] = cluster_info[0]['id']

    cli_cmd = ''
    cluster_job_ids = [job_name]

    if shutdown:
        poll_cluster = True if not dryrun else False
        # put marker file in s3 to initiate streaming job shutdown
        shutdown_initiated = shutdown_streaming_job(aws_api, config, job_name, checkpoint_bucket)
        if not shutdown_initiated:
            cluster_job_ids = []

        # once the streaming job shutdown finishes, this job will copy the spark checkpoints to s3
        copy_job_id = 'S3DistCp'
        aws_api.add_checkpoint_copy_job(config, copy_job_id)
        cluster_job_ids.append(copy_job_id)

    if artifact_path and not shutdown:  # submit a new EMR Step to the running cluster
        config['artifact_parts'] = get_artifact_parts(artifact_path)
        ec2_instances = aws_api.list_running_cluster_instances(config['cluster_id'])
        config['num_executors'] = len(ec2_instances['Instances']) - 2  # exclude master node and cluster driver node
        config['step_args'] = tokenize_emr_step_args(spark_template.render(config))

        private_ips = [i['PrivateIpAddress'] for i in ec2_instances['Instances']]
        log_msg = "environment={}, cluster={}, job={}, action=list-cluster-instances, count={}, ips={}" \
            .format(env, cluster_name, job_name, len(private_ips), json.dumps(private_ips))
        log_assertion(len(private_ips) > 0, log_msg,
                      'Expected 1+ but found 0 instances for cluster with name {}'.format(cluster_name))
        artifact_payload = \
            {'runtime': job_runtime, 'bucket': config['artifact_parts'][0], 'key': config['artifact_parts'][1]}

        api_log = "environment={}, cluster={}, job={}, action={}, bucket={}, key={}, ip={}, status_code={}, message={}"

        # download artifacts and install dependencies to the EMR nodes by calling the bootstrapped web service
        download_requests = []
        install_requests = []
        for ip in private_ips:
            if dryrun:
                continue
            r = grequests.post('http://{}:8080/download'.format(ip), json=artifact_payload)
            download_requests.append(r)
            if job_runtime.lower() == 'python':
                r = grequests.get('http://{}:8080/requirements'.format(ip))
                install_requests.append(r)

        download_responses = zip(private_ips, grequests.map(download_requests, size=7))
        validate_responses(download_responses, api_log, config, 'download')

        install_responses = zip(private_ips, grequests.map(install_requests, size=7))
        validate_responses(install_responses, api_log, config, 'install')

        cli_cmd = add_spark_step_template.render(config)
        logging.info('\n\n{}'.format(cli_cmd))
        if not dryrun:
            output = run_cli_cmd(cli_cmd)
            logging.info(output)
            log_msg = "environment={}, cluster={}, job={}, action=add-job-step".format(env, cluster_name, job_name)
            log_assertion('StepIds' in json.loads(output).keys(), log_msg, 'Key \'StepIds\' not found in shell output')

    if poll_cluster:  # monitor state of the EMR Steps (Spark Jobs). Supports multiple jobs in sequence
        for job_id in cluster_job_ids:
            job_state = 'UNKNOWN'

            while job_state != 'COMPLETED':
                jobs = aws_api.list_cluster_steps(config['cluster_id'], job_id, active_only=False)
                if job_state == 'UNKNOWN':
                    log_msg = "environment={}, cluster={}, job={}, action=list-cluster-steps, clusterId={}, " \
                              "numSteps={}".format(env, cluster_name, job_id, config['cluster_id'], len(jobs))
                    log_assertion(len(jobs) > 0, log_msg,
                                  'Expected 1+ but found {} jobs for name {}'.format(len(jobs), job_id))
                current_job = reduce((lambda j1, j2: j1 if j1['Status']['Timeline']['CreationDateTime'] >
                                                           j2['Status']['Timeline']['CreationDateTime'] else j2), jobs)
                job_metrics = cluster_step_metrics(current_job)
                logging.info("environment={}, cluster={}, job={}, action=poll-cluster, stepId={}, state={}, "
                             "createdTime={}, minutesElapsed={}".format(env, cluster_name, job_id, job_metrics['id'],
                                                                        job_metrics['state'],
                                                                        job_metrics['createdTime'],
                                                                        job_metrics['minutesElapsed']))
                job_state = job_metrics['state']
                minutes_elapsed = job_metrics['minutesElapsed']

                # check for termination events: failure or timeout exceeded
                if job_metrics['state'] == 'FAILED':
                    log_msg = "environment={}, cluster={}, job={}, action=exit-failed-state, stepId={}, state={}" \
                        .format(env, cluster_name, job_id, job_metrics['id'], job_metrics['state'])
                    log_assertion(job_metrics['state'] != 'FAILED', log_msg,
                                  'Job in unexpected state {}'.format(job_metrics['state']))
                elif minutes_elapsed > job_timeout:
                    log_msg = "environment={}, cluster={}, job={}, action=exceeded-timeout, minutes={}" \
                        .format(env, cluster_name, job_id, job_timeout)
                    log_assertion(minutes_elapsed < job_timeout, log_msg, 'Job exceeded timeout {}'.format(job_timeout))
                time.sleep(60)
        if shutdown:
            aws_api.delete_job_shutdown_marker(checkpoint_bucket, job_name)
        if auto_terminate:
            aws_api.terminate_clusters(cluster_name, config)
    return cli_cmd if cli_cmd else None


def validate_responses(responses, api_log, config, action):
    status_code_assertion = 'Excepted 200 but found {} HTTP status code'
    for resp in responses:
        log_msg = api_log.format(config['env'], config['cluster_name'], config['job_name'], action,
                                 config['artifact_parts'][0], config['artifact_parts'][1], resp[0], resp[1].status_code,
                                 try_response_json_lookup(resp[1], 'message'))
        log_assertion(resp[1].status_code == 200, log_msg, status_code_assertion.format(resp[1].status_code))


def cluster_step_metrics(step_info):
    created_dt = step_info['Status']['Timeline']['CreationDateTime']
    str_created_dt = created_dt.strftime("%Y-%m-%dT%H-%M-%S")
    seconds_elapsed = (datetime.now(pytz.utc) - created_dt).total_seconds()
    minutes_elapsed = divmod(seconds_elapsed, 60)[0]
    return {
        'id': step_info['Id'],
        'name': step_info['Name'],
        'state': step_info['Status']['State'],
        'createdTime': str_created_dt,
        'minutesElapsed': minutes_elapsed
    }


def shutdown_streaming_job(api, configs, job_name, checkpoint_s3_bucket):
    jobs = api.list_cluster_steps(configs['cluster_id'], job_name, active_only=True)
    count = len(jobs)

    log_msg = "environment={}, cluster={}, job={}, action=shutdown, is_currently_active={}"
    if count == 1:
        api.put_job_shutdown_marker(checkpoint_s3_bucket, job_name)
        logging.info(log_msg.format(configs['env'], configs['cluster_name'], job_name, True))
        return True  # shutdown was initiated with marker file
    else:
        logging.info(log_msg.format(configs['env'], configs['cluster_name'], job_name, False))
        return False


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)
    parse_arguments()

import click
import json
import logging
import pytz
import requests
import sys
import time
from datetime import datetime
from jinja2 import Template

from templates import spark_template
from utils import *

emr_add_step_template = Template('aws emr add-steps{% if not airflow %} --profile {{ profile }}{% endif %} '
                                 '--cluster-id {{ cluster_id }} '
                                 '--steps Type=Spark,Name={{ job_name }},ActionOnFailure=CONTINUE,Args={{ step_args }}')


@click.command()
@click.pass_context
@click.option('--env', prompt='Enter the environment to deploy to (qa/pre-prod/prod)',
              help='Environment to deploy to (required).')
@click.option('--job-name', help='Name for the EMR Step & Spark job.')
@click.option('--job-runtime', default='scala', help='Runtime for Spark, should be either Scala or Python.')
@click.option('--job-timeout', default=60, help='Spark job timeout in minutes.')
@click.option('--cluster-name', default='DataPipeline', help='Name for the EMR cluster.')
@click.option('--artifact-path', help='Amazon S3 path to the Spark artifact.')
@click.option('--poll-cluster', is_flag=True, help='Option to poll the cluster for job state (completed/failed).')
@click.option('--auto-terminate', is_flag=True, help='Terminate the cluster after the Spark job finishes.')
@click.option('--cicd', is_flag=True, help='Indicator for deployment from gocd; uses IAM profile auth.')
@click.option('--airflow', is_flag=True, help='Indicator for deployment from airflow; uses EC2 instance role auth.')
@click.option('--dryrun', is_flag=True, help='Print out the EMR command without actually running it.')
@click.option('--job-args', default='', help='Extra arguments for the Spark application.')
@click.option('--job-configs', default='', help='Extra configs for the Spark application.')
@click.option('--main-class', help='Main class of the Spark application.')
def parse_arguments(ctx, env, job_name, job_runtime, job_timeout, cluster_name, artifact_path, poll_cluster,
                    auto_terminate, cicd, airflow, dryrun, job_args, job_configs, main_class):
    handle_job_request(ctx, env, job_name, job_runtime, job_timeout, cluster_name, artifact_path, poll_cluster,
                       auto_terminate, cicd, airflow, dryrun, None, job_args, job_configs, main_class)


def handle_job_request(ctx, env, job_name, job_runtime, job_timeout, cluster_name, artifact_path, poll_cluster,
                       auto_terminate, cicd, airflow, dryrun, api, job_args=None, job_configs=None, main_class=None):
    config = ctx.params
    log_msg = 'environment={}, cluster={}, job={}, action=check-runtime, runtime={}' \
        .format(env, cluster_name, job_name, job_runtime)
    log_assertion(job_runtime.lower() in valid_runtimes, log_msg, 'runtime should be either Scala or Python')

    # determine aws profile to use for AWSApi
    config['profile'] = get_aws_profile(env, airflow, cicd)
    aws_api = api if api is not None else AWSApi(config['profile'])

    # get existing cluster info
    cluster_info = aws_api.get_emr_cluster_with_name(cluster_name)
    log_msg = "environment={}, cluster={}, job={}, action=get-clusters, count={}, clusterList={}" \
        .format(env, cluster_name, job_name, len(cluster_info), json.dumps(cluster_info))
    log_assertion(len(cluster_info) == 1, log_msg,
                  'Expected 1 but found {} running clusters with name {}'.format(len(cluster_info), cluster_name))

    # add cluster id to the config
    config['cluster_id'] = cluster_info[0]['id']

    if artifact_path:  # submit a new EMR Step to the running cluster
        config['artifact_parts'] = get_artifact_parts(artifact_path)
        config['step_args'] = tokenize_emr_step_args(spark_template.render(config))

        ec2_instances = aws_api.list_cluster_instances(config['cluster_id'])
        private_ips = [i['PrivateIpAddress'] for i in ec2_instances['Instances']]
        log_msg = "environment={}, cluster={}, job={}, action=list-cluster-instances, count={}, ips={}" \
            .format(env, cluster_name, job_name, len(private_ips), json.dumps(private_ips))
        log_assertion(len(private_ips) > 0, log_msg,
                      'Expected 1+ but found 0 instances for cluster with name {}'.format(cluster_name))
        artifact_payload = \
            {'runtime': job_runtime, 'bucket': config['artifact_parts'][0], 'key': config['artifact_parts'][1]}

        api_log = "environment={}, cluster={}, job={}, action={}, bucket={}, key={}, ip={}, status_code={}, message={}"
        status_code_assertion = 'Excepted 200 but found {} HTTP status code'
        for ip in private_ips:
            r = requests.post('http://{}:8080/download'.format(ip), json=artifact_payload)
            log_msg = api_log.format(env, cluster_name, job_name, 'download', config['artifact_parts'][0],
                                     config['artifact_parts'][1], ip, r.status_code, r.json()['message'])
            log_assertion(r.status_code == 200, log_msg, status_code_assertion.format(r.status_code))
            if job_runtime.lower() == 'python':
                r = requests.get('http://{}:8080/requirements'.format(ip))
                log_msg = api_log.format(env, cluster_name, job_name, 'requirements', config['artifact_parts'][0],
                                         config['artifact_parts'][1], ip, r.status_code, r.json()['message'])
                log_assertion(r.status_code == 200, log_msg, status_code_assertion.format(r.status_code))

        cli_cmd = emr_add_step_template.render(config)
        print '\n\n{}'.format(cli_cmd)
        output = run_cli_cmd(cli_cmd)
        print output
        log_msg = "environment={}, cluster={}, job={}, action=add-job-step".format(env, cluster_name, job_name)
        log_assertion('StepIds' in json.loads(output).keys(), log_msg, 'Key \'StepIds\' not found in shell output')

    if poll_cluster:  # monitor state of the EMR Steps (Spark Jobs)
        minutes_elapsed = 0
        while minutes_elapsed <= job_timeout:
            response = aws_api.list_cluster_steps(config['cluster_id'])
            jobs = [s for s in response['Steps'] if s['Name'] == job_name]
            if minutes_elapsed == 0:
                log_msg = "environment={}, cluster={}, job={}, action=list-cluster-steps, clusterId={}, numSteps={}" \
                    .format(env, cluster_name, job_name, config['cluster_id'], len(jobs))
                log_assertion(len(jobs) == 1, log_msg,
                              'Expected 1 but found {} jobs for name {}'.format(len(jobs), job_name))

            job_metrics = cluster_step_metrics(jobs[0])
            logging.info("environment={}, cluster={}, job={}, action=poll-cluster, stepId={}, state={}, "
                         "createdTime={}, minutesElapsed={}".format(env, cluster_name, job_name, job_metrics['id'],
                                                                    job_metrics['state'],
                                                                    job_metrics['createdTime'],
                                                                    job_metrics['minutesElapsed']))
            if job_metrics['state'] == 'COMPLETED':
                if auto_terminate:
                    aws_api.terminate_clusters(cluster_name, config)
                sys.exit(0)  # job successful
            elif job_metrics['state'] == 'FAILED':
                log_msg = "environment={}, cluster={}, job={}, action=exit-failed-state, stepId={}, state={}".format(
                    env, cluster_name, job_name, job_metrics['id'], job_metrics['state']
                )
                log_assertion(job_metrics['state'] != 'FAILED', log_msg,
                              'Job in unexpected state {}'.format(job_metrics['state']))
            minutes_elapsed = job_metrics['minutesElapsed']
            time.sleep(60)
        log_msg = "environment={}, cluster={}, job={}, action=exceeded-timeout, minutes={}" \
            .format(env, cluster_name, job_name, job_timeout)
        log_assertion(minutes_elapsed < job_timeout, log_msg, 'Job exceeded timeout {}'.format(job_timeout))


def cluster_step_metrics(step_info):
    created_dt = step_info['Status']['Timeline']['CreationDateTime']
    str_created_dt = created_dt.strftime("%Y-%m-%dT%H-%M-%S")
    seconds_elapsed = (datetime.now(pytz.utc) - created_dt).seconds
    minutes_elapsed = divmod(seconds_elapsed, 60)[0]
    return {
        'id': step_info['Id'],
        'name': step_info['Name'],
        'state': step_info['Status']['State'],
        'createdTime': str_created_dt,
        'minutesElapsed': minutes_elapsed
    }


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)
    parse_arguments()

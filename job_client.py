import click
import logging
import sys
import json
import time
import pytz
import requests
from utils import *
from datetime import datetime, timedelta
from jinja2 import Template
from templates import spark_template

emr_add_step_template = Template('aws emr add-steps{% if not airflow %} --profile {{ profile }}{% endif %} '
                                 '--cluster-id {{ cluster_id }} '
                                 '--steps Type=Spark,Name={{ job_name }},ActionOnFailure=CONTINUE,Args={{ step_args }}')


@click.command()
@click.pass_context
@click.option('--env', prompt='Enter the environment to deploy to (qa/pre-prod/prod)',
              help='Environment to deploy to (required).')
@click.option('--job-name', help='Name for the EMR Step & Spark job.')
@click.option('--job-runtime', default='scala', help='Runtime for Spark, should be either Scala or Python.')
@click.option('--job-args', default='', help='Extra arguments for the Spark application.')
@click.option('--job-timeout', default=60, help='Spark job timeout in minutes.')
@click.option('--cluster-name', default='DataPipeline', help='Name for the EMR cluster.')
@click.option('--main-class', help='Main class of the Spark application.')
@click.option('--artifact-path', help='Amazon S3 path to the Spark artifact.')
@click.option('--h2o-backend', is_flag=True, help='Indicates that the Spark job uses the H2O backend.')
@click.option('--poll-cluster', is_flag=True, help='Option to poll the cluster for job state (completed/failed).')
@click.option('--auto-terminate', is_flag=True, help='Terminate the cluster after the Spark job finishes.')
@click.option('--airflow', is_flag=True, help='Indicator for deployment from airflow; uses EC2 instance role auth.')
def handle_job_request(ctx, env, job_name, job_runtime, job_args, job_timeout, cluster_name,
                       main_class, artifact_path, h2o_backend, poll_cluster, auto_terminate, airflow):
    config = ctx.params
    config['profile'] = profiles[env]
    # initialize the aws clients
    s3_client, emr_client = get_clients(None if airflow else profiles[env])

    # get existing cluster info
    cluster_info = get_emr_cluster_with_name(emr_client, cluster_name)
    log_msg = "environment={}, cluster={}, job={}, action=get-clusters, count={}, clusterList={}" \
        .format(env, cluster_name, job_name, len(cluster_info), json.dumps(cluster_info))
    log_assertion(len(cluster_info) == 1, log_msg)

    # add cluster id to the config
    config['cluster_id'] = cluster_info[0]['id']

    if artifact_path:  # submit a new EMR Step to the running cluster
        config['step_args'] = tokenize_emr_step_args(spark_template.render(config))

        ec2_instances = emr_client.list_instances(
            ClusterId=config['cluster_id']
        )
        private_ips = [i['PrivateIpAddress'] for i in ec2_instances['Instances']]
        artifact_parts = artifact_path.replace('s3://', '').split('/', 1)
        artifact_payload = {'runtime': job_runtime, 'bucket': artifact_parts[0], 'key': artifact_parts[1]}

        api_log = "environment={}, cluster={}, job={}, action={}, bucket={}, key={}, ip={}, status_code={}, message={}"
        for ip in private_ips:
            r = requests.post('http://{}:8080/download'.format(ip), json=artifact_payload)
            log_msg = api_log.format(env, cluster_name, job_name, 'download', artifact_parts[0],
                                     artifact_parts[1], ip, r.status_code, r.json()['message'])
            log_assertion(r.status_code == 200, log_msg)
            if job_runtime.lower() == 'python':
                r = requests.get('http://{}:8080/requirements'.format(ip))
                log_msg = api_log.format(env, cluster_name, job_name, 'requirements', artifact_parts[0],
                                         artifact_parts[1], ip, r.status_code, r.json()['message'])
                log_assertion(r.status_code == 200, log_msg)

        cli_cmd = emr_add_step_template.render(config)
        print '\n\n{}'.format(cli_cmd)
        output = run_cli_cmd(cli_cmd)
        print output
        log_msg = "environment={}, cluster={}, job={}, action=add-job-step".format(env, cluster_name, job_name)
        log_assertion('StepIds' in json.loads(output).keys(), log_msg)

    if poll_cluster:  # monitor state of the EMR Steps (Spark Jobs)
        minutes_elapsed = 0
        while minutes_elapsed <= job_timeout:
            response = emr_client.list_steps(
                ClusterId=config['cluster_id']
            )
            jobs = [s for s in response['Steps'] if s['Name'] == job_name]
            if minutes_elapsed == 0:
                log_msg = "environment={}, cluster={}, job={}, action=list-cluster-steps, clusterId={}, numSteps={}" \
                    .format(env, cluster_name, job_name, config['cluster_id'], len(jobs))
                log_assertion(len(jobs) == 1, log_msg)

            job_metrics = cluster_step_metrics(jobs[0])
            logging.info("environment={}, cluster={}, job={}, action=poll-cluster, stepId={}, state={}, "
                         "createdTime={}, minutesElapsed={}".format(env, cluster_name, job_name, job_metrics['id'],
                                                                    job_metrics['state'],
                                                                    job_metrics['createdTime'],
                                                                    job_metrics['minutesElapsed']))
            if job_metrics['state'] == 'COMPLETED':
                if auto_terminate:
                    terminate_clusters(emr_client, cluster_name, config)
                sys.exit(0)  # job successful
            elif job_metrics['state'] == 'FAILED':
                log_msg = "environment={}, cluster={}, job={}, action=exit-failed-state, stepId={}, state={}".format(
                    env, cluster_name, job_name, job_metrics['id'], job_metrics['state']
                )
                log_assertion(job_metrics['state'] != 'FAILED', log_msg)
            minutes_elapsed = job_metrics['minutesElapsed']
            time.sleep(60)
        log_msg = "environment={}, cluster={}, job={}, action=exceeded-timeout, minutes={}" \
            .format(env, cluster_name, job_name, job_timeout)
        log_assertion(minutes_elapsed < job_timeout, log_msg)


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
    handle_job_request()

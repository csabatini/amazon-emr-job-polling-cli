import click
import logging
import sys
import json
import time
import pytz
from datetime import datetime, timedelta
from jinja2 import Template
from awsutils import profiles, get_clients, get_emr_cluster_with_name, tokenize_emr_step_args, terminate_clusters
from templates import spark_template

emr_add_step_template = Template('aws emr --profile {{ env }} add-steps --cluster-id {{ cluster_id }} '
                                 '--steps Type=Spark,Name={{ job_name }},ActionOnFailure=CONTINUE,Args={{ step_args }}')


@click.command()
@click.pass_context
@click.option('--env', prompt='Enter the environment to deploy to (qa/pre-prod/prod)',
              help='Environment to deploy to (required).')
@click.option('--job_name', help='Name for the EMR Step & Spark job.')
@click.option('--job_runtime', default='scala', help='Runtime for Spark, should be either Scala or Python.')
@click.option('--job_timeout', default=120, help='Spark job timeout in minutes.')
@click.option('--cluster_name', default='DataPipeline', help='Name for the EMR cluster.')
@click.option('--main_class', default='com.sonicdrivein.datapipeline.Main', help='Main class of the Spark application.')
@click.option('--artifact_path', help='Amazon S3 path to the Spark artifact.')
@click.option('--h2o_backend', is_flag=True, help='Indicates that the Spark job uses the H2O backend.')
@click.option('--poll-steps', is_flag=True, help='Option to describe the state of the steps on a cluster.')
@click.option('--airflow', is_flag=True, help='Indicator for deployment from airflow; uses EC2 instance role auth.')
def handle_job_request(ctx, env, job_name, job_runtime, job_timeout, cluster_name,
                       main_class, artifact_path, h2o_backend, poll_steps, airflow):
    config = ctx.params
    config['profile'] = profiles[env]
    # initialize the aws clients
    s3_client, emr_client = get_clients(None if airflow else profiles[env])

    # get existing cluster info
    cluster_info = get_emr_cluster_with_name(emr_client, cluster_name)
    assert len(cluster_info) == 1, \
        "expected one but found {} cluster(s) with name {}".format(len(cluster_info), cluster_name)

    # add cluster id to the config and log cluster details
    config['cluster_id'] = cluster_info[0]['id']
    cluster_info_json = json.dumps(cluster_info)
    logging.info("action=get-clusters, count={}, clusterList={}".format(len(cluster_info),
                                                                        cluster_info_json))
    if poll_steps:  # monitor state of the EMR Steps (Spark Jobs)
        minutes_elapsed = 0
        while minutes_elapsed <= job_timeout:
            response = emr_client.list_steps(
                ClusterId=config['cluster_id']
            )
            if minutes_elapsed == 0:
                logging.info("action=list-cluster-steps, clusterId={}, numSteps={}".format(config['cluster_id'],
                                                                                           len(response['Steps'])))
            jobs = [s for s in response['Steps'] if s['Name'] == job_name]
            assert len(jobs) == 1, \
                "expected one but found {} job(s) with name {}".format(len(job_steps), job_name)
            job_metrics = cluster_step_metrics(jobs[0])
            logging.info("action=poll-cluster-step, stepId={}, stepName={}, state={}, createdTime={}, minutesElapsed={}"
                         .format(job_metrics['id'], job_metrics['name'], job_metrics['state'],
                                 job_metrics['createdTime'], job_metrics['minutesElapsed']))
            if job_metrics['state'] == 'COMPLETED':
                terminate_clusters(emr_client, cluster_name, config)
                sys.exit(0)  # job successfuly
            elif job_metrics['state'] in ['CANCELLED', 'FAILED', 'INTERRUPTED']:
                raise ValueError('Job in unexpected state: {}'.format(job_metrics['state']))
            minutes_elapsed = job_metrics['minutesElapsed']
            time.sleep(60)
        raise ValueError('Job exceeded timeout: {} minutes'.format(job_timeout))

    else:  # TODO add a step to the cluster - this needs to be explored, currently just creating 1 cluster per step
        config['step_args'] = tokenize_emr_step_args(spark_template.render(config))

        cli_cmd = emr_add_step_template.render(config)
        print '\n\n{}'.format(cli_cmd)
        # output = run_cli_cmd(cli_cmd)
        # print output
        # assert 'StepId' in json.loads(output).keys(), 'Failed to add step to cluster'


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

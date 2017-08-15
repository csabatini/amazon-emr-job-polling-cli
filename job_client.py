import click
import logging
import sys
import json
from jinja2 import Template
from awsutils import get_clients, get_emr_cluster_with_name, tokenize_emr_step_args, run_cli_cmd
from templates import spark_template

emr_add_step_template = Template('aws emr --profile {{ env }} add-steps --cluster-id {{ cluster_id }} '
                                 '--steps Type=Spark,Name={{ job_name }},ActionOnFailure=CONTINUE,Args={{ step_args }}')


@click.command()
@click.pass_context
@click.option('--env', prompt='Enter the environment to deploy to (qa/pre-prod/prod)',
              help='Environment to deploy to (required).')
@click.option('--job_name', default='DataPipeline', help='Name for the Spark job.')
@click.option('--cluster_name', default='DataPipeline', help='Name for the EMR cluster.')
@click.option('--main_class', default='com.sonicdrivein.datapipeline.Main', help='Main class of the Spark application.')
@click.option('--artifact_path', default='s3://qa-sonic-enterprise-data-hub/artifacts/pipeline.jar',
              help='Amazon S3 path to the Spark artifact.')
@click.option('--h2o_backend', is_flag=True, help='Indicates that the Spark job uses the H2O backend.')
def handle_job_request(ctx, env, job_name, cluster_name, main_class, artifact_path, h2o_backend):
    config = ctx.params
    if artifact_path:
        # initialize the aws clients
        s3_client, emr_client = get_clients(env)

        # get existing cluster info
        cluster_info = get_emr_cluster_with_name(emr_client, cluster_name)
        assert len(cluster_info) == 1, \
            "expected one but found {} cluster(s) with name {}".format(len(cluster_info), cluster_name)
        config['cluster_id'] = cluster_info[0]['id']
        cluster_info_json = json.dumps(cluster_info)
        logging.info("action=get-clusters, result=success, count={}, value={}".format(len(cluster_info),
                                                                                      cluster_info_json))
        config['step_args'] = tokenize_emr_step_args(spark_template.render(config))

        cli_cmd = emr_add_step_template.render(config)
        print '\n\n{}'.format(cli_cmd)
        # output = run_cli_cmd(cli_cmd)
        # print output
        # assert 'StepId' in json.loads(output).keys(), 'Failed to add step to cluster'
    else:
        return None


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    handle_job_request()

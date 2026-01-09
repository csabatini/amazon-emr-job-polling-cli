import sys
import json
import click
import logging
from jinja2 import Template
from utils import profiles, terminate_clusters, get_clients, tokenize_emr_step_args, run_cli_cmd, log_assertion
from templates import spark_template

valid_runtimes = ['scala', 'python']
emr_create_cli_template = Template('''aws emr create-cluster{% if not airflow %} --profile {{ profile }}{% endif %}
    \t--name {{ cluster_name }}
    \t--release-label {{ emr_version }}
    \t--configurations file://configurations.json
    \t--{{ term_choice }}
    \t--service-role {{ emr_service_role }}
    \t--tags env={{ env }} owner=bigdata project={{ project }} Name={{ cluster_name }}
    \t--applications Name=Hadoop Name=Spark Name=Hue 
    \t--bootstrap-actions Path=s3://{{ env }}-sonic-artifacts/scripts/{% if job_runtime.lower() == "python" %}pyspark_bootstrap.sh,Name=PysparkBootstrap,Args=[{{ artifact_path }}]{% else %}download_service.sh,Name=S3DownloadService,Args=[{{ artifact_path }}]{% endif %} 
    \t--ec2-attributes InstanceProfile={{ emr_instance_profile }},KeyName={{ emr_ssh_key_name }},SubnetIds=[\'{{ master_private_subnet_app_secondary_id }}\'],ServiceAccessSecurityGroup={{ master_emrservice_security_group_id }},EmrManagedMasterSecurityGroup={{ master_emr_security_group_id }},EmrManagedSlaveSecurityGroup={{ master_emr_security_group_id }} 
    \t--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType={{ master_type }} InstanceGroupType=CORE,InstanceType={{ core_type }},InstanceCount={{ core_count }} 
    \t{% if artifact_path %}--steps Type=Spark,Name={{ job_name }},ActionOnFailure=CONTINUE,Args={{ step_args }}{% endif %}
''')


@click.command()
@click.pass_context
@click.option('--env', prompt='Enter the environment to deploy to (qa/pre-prod/prod)',
              help='Environment to deploy to (required).')
@click.option('--emr_version', default='emr-5.6.0', help='Service version for EMR.')
@click.option('--job_name', help='Name for the EMR Step & Spark job')
@click.option('--job_runtime', default='scala', help='Runtime for Spark, should be either Scala or Python.')
@click.option('--job_args', default='', help='Extra arguments for the Spark application.')
@click.option('--cluster_name', default='DataPipeline', help='Name for the EMR cluster.')
@click.option('--project', default='know_your_customer', help='Project to tag the AWS resources with.')
@click.option('--main_class', default='com.sonicdrivein.datapipeline.Main', help='Main class of the Spark application.')
@click.option('--artifact_path', help='Amazon S3 path to the Spark artifact.')
@click.option('--core_count', default=2, help='Number of core cluster nodes.')
@click.option('--core_type', default='m3.xlarge', help='EC2 instance type for the core node(s).')
@click.option('--master_type', default='m1.large', help='EC2 instance type for the master node.')
@click.option('--h2o_backend', is_flag=True, help='Indicates that the Spark job uses the H2O backend.')
@click.option('--auto-terminate', 'term_choice', flag_value='auto-terminate',
              help='Terminate the cluster after the Spark job finishes.')
@click.option('--no-auto-terminate', 'term_choice', flag_value='no-auto-terminate', default=True,
              help='Keep the cluster running after the Spark job finishes (default).')
@click.option('--cicd', is_flag=True, help='Indicator for deployment from gocd; uses IAM profile auth.')
@click.option('--airflow', is_flag=True, help='Indicator for deployment from airflow; uses EC2 instance role auth.')
def deploy(ctx, env, emr_version, job_name, job_runtime, job_args, cluster_name, project, main_class,
           artifact_path, core_count, core_type, master_type, h2o_backend, term_choice, cicd, airflow):
    config = ctx.params
    assert job_runtime.lower() in valid_runtimes, 'job_runtime must be either Scala or Python'
    config['step_args'] = None if not artifact_path else tokenize_emr_step_args(spark_template.render(config))
    config['profile'] = 'bdp-{}'.format(env.replace('-', '')) if cicd else profiles[env]  # gocd profiles are bdp-{env}

    # create the boto clients for this environment
    s3_client, emr_client = get_clients(None if airflow else config['profile'])

    terminate_clusters(emr_client, cluster_name, config)

    # read terrraform state for the account (IAM)
    get_s3_state(s3_client, env, config, '{}-master/account.tfstate'.format(env),
                 ['emr_service_role', 'emr_instance_profile', 'emr_ssh_key_name'])
    # read terraform state for the network (VPC)
    get_s3_state(s3_client, env, config, '{}-master/shared.tfstate'.format(env),
                 ['master_private_subnet_app_secondary_id', 'master_emrservice_security_group_id',
                  'master_emr_security_group_id'])

    if not cicd and not airflow:
        print '\n{}\n\nPress enter to confirm the cluster config'.format(json.dumps(config, indent=4, sort_keys=True)),
        raw_input()

    cli_cmd = emr_create_cli_template.render(config)
    logging.info('\nRunning script: \n\n{}\n\n'.format(cli_cmd))
    output = run_cli_cmd(cli_cmd)
    logging.info(output)
    log_msg = "environment={}, cluster={}, job={}, action=provision-cluster".format(env, cluster_name, job_name)
    log_assertion('ClusterId' in json.loads(output).keys(), log_msg)


def get_s3_state(s3client, environment, config, s3_key, output_keys):
    obj = s3client.get_object(Bucket='{}-sonic-terraform-state-files'.format(environment), Key=s3_key)

    state = json.loads(obj['Body'].read())
    outputs = state['modules'][0]['outputs']
    for k in output_keys:
        config[k] = outputs[k]['value']


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)
    deploy()

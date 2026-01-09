from mock import Mock
from emr.job_client import handle_job_request
import copy
import pytest

params = {
    "airflow": False,
    "artifact_path": "s3://us-east-1.elasticmapreduce/samples/wordcount/",
    "checkpoint_bucket": "",
    "cicd": False,
    "cluster_name": "Sandbox",
    "job_name": "WordCount",
    "job_runtime": "Scala",
    "job_timeout": 60,
    "job_mode": "batch",
    "poll_cluster": False,
    "profile": "qa",
    "shutdown": False,
    "dryrun": True,
    "emr_version": "emr-5.6.0",
    "env": "qa",
    "job_args": "",
    "main_class": "org.apache.hadoop.tools.WordCount",
    "profile": "cicd-qa",
    "project": "",
    "terminate": True
}


@pytest.fixture
def aws_api():
    mock_api = Mock()
    mock_api.get_emr_cluster_with_name.return_value = [{'id': 'cluster123'}]
    mock_api.is_cluster_active.return_value = False
    mock_api.list_cluster_steps.return_value = ['step123']
    mock_api.get_remote_state_values.return_value = {
        'master_private_subnet_app_secondary_id': 'subnet-3c2f6611',
        'master_emrservice_security_group_id': 'sg-85e456f4',
        'master_emr_security_group_id': 'sg-48209339'
    }
    return mock_api


def test_succeeds_with_default_params(aws_api):
    output = handle_job_request(params, aws_api)
    assert len(output) > 0
    assert output == "aws emr add-steps --profile cicd-qa --cluster-id cluster123 --steps Type=Spark,Name=WordCount,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,--conf,'spark.app.name=BTYD',--conf,'spark.yarn.max.executor.failures=8',--conf,'spark.yarn.appMasterEnv.ENVIRONMENT=qa',--py-files,s3://qa-sonic-enterprise-data-hub/artifacts/btyd-1.0.0/application.zip"


# def test_shutdown_false_no_shutdown_api_calls():
    # mock = get_mock_api()
    # handle_job_request(params, mock)  # shutdown=False

    # # api.put_job_shutdown_marker not called
    # assert mock.put_job_shutdown_marker.call_count == 0

    # # api.add_checkpoint_copy_job not called
    # assert mock.add_checkpoint_copy_job.call_count == 0


# def test_shutdown_true_makes_shutdown_api_calls():
    # mock = get_mock_api()
    # test_params = copy.deepcopy(params)
    # test_params['shutdown'] = True

    # handle_job_request(test_params, mock)

    # # api.put_job_shutdown_marker is called for this job
    # calls = [call('qa-checkpoints', 'BTYD')]
    # assert mock.put_job_shutdown_marker.call_count == 1
    # mock.put_job_shutdown_marker.assert_has_calls(calls)

    # # api.add_checkpoint_copy_job is called
    # assert mock.add_checkpoint_copy_job.call_count == 1


# def test_cluster_job_not_added_conditions():
    # mock = get_mock_api()
    # test_params = copy.deepcopy(params)

    # # job should not be added if either artifact_path is empty, or shutdown is True
    # test_params['artifact_path'] = 's3://qa-sonic-enterprise-data-hub/artifacts/btyd-1.0.0/'
    # test_params['shutdown'] = True
    # handle_job_request(test_params, mock)

    # # this API call_count will be 0 unless the job is added
    # assert mock.list_running_cluster_instances.call_count == 0

    # test_params['artifact_path'] = ''
    # test_params['shutdown'] = False
    # handle_job_request(test_params, mock)

    # assert mock.list_running_cluster_instances.call_count == 0

    # # job should be added under these parameters
    # test_params['artifact_path'] = 's3://qa-sonic-enterprise-data-hub/artifacts/btyd-1.0.0/'
    # test_params['shutdown'] = False
    # handle_job_request(test_params, mock)

    # assert mock.list_running_cluster_instances.call_count == 1



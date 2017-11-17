from mock import call, Mock
from emr.job_client import handle_job_request
import copy

params = {
    "airflow": False,
    "cicd": False,
    "cluster_name": "Sandbox",
    "job_name": "BTYD",
    "job_runtime": "python",
    "job_timeout": 60,
    "job_mode": "batch",
    "poll_cluster": False,
    "artifact_path": "s3://qa-sonic-enterprise-data-hub/artifacts/btyd-1.0.0/",
    "checkpoint_bucket": "",
    "shutdown": False,
    "dryrun": True,
    "emr_version": "emr-5.6.0",
    "env": "qa",
    "h2o_backend": False,
    "job_args": "",
    "main_class": "com.sonicdrivein.datapipeline.Main",
    "project": "know_your_customer",
    "auto_terminate": True
}


def get_mock_api():
    mock_api = Mock()
    mock_api.get_emr_cluster_with_name.return_value = [{'id': 'cluster123'}]
    mock_api.list_cluster_steps.return_value = ['step123']
    mock_api.list_running_cluster_instances.return_value = {
        'Instances': [
            {'PrivateIpAddress': '192.168.0.1'},
            {'PrivateIpAddress': '192.168.0.2'},
            {'PrivateIpAddress': '192.168.0.3'}
        ]
    }
    return mock_api


def test_succeeds_with_default_params():
    mock = get_mock_api()
    output = handle_job_request(params, mock)
    assert len(output) > 0
    assert output == "aws emr add-steps --profile qa --cluster-id cluster123 --steps Type=Spark,Name=BTYD,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,--conf,'spark.app.name=BTYD',--conf,'spark.yarn.max.executor.failures=8',--conf,'spark.yarn.appMasterEnv.ENVIRONMENT=qa',--py-files,s3://qa-sonic-enterprise-data-hub/artifacts/btyd-1.0.0/application.zip,s3://qa-sonic-enterprise-data-hub/artifacts/btyd-1.0.0/main.py]"


def test_shutdown_false_no_shutdown_api_calls():
    mock = get_mock_api()
    handle_job_request(params, mock)  # shutdown=False

    # api.put_job_shutdown_marker not called
    assert mock.put_job_shutdown_marker.call_count == 0

    # api.add_checkpoint_copy_job not called
    assert mock.add_checkpoint_copy_job.call_count == 0


def test_shutdown_true_makes_shutdown_api_calls():
    mock = get_mock_api()
    test_params = copy.deepcopy(params)
    test_params['shutdown'] = True

    handle_job_request(test_params, mock)

    # api.put_job_shutdown_marker is called for this job
    calls = [call('qa-checkpoints', 'BTYD')]
    assert mock.put_job_shutdown_marker.call_count == 1
    mock.put_job_shutdown_marker.assert_has_calls(calls)

    # api.add_checkpoint_copy_job is called
    assert mock.add_checkpoint_copy_job.call_count == 1


def test_cluster_job_not_added_conditions():
    mock = get_mock_api()
    test_params = copy.deepcopy(params)

    # job should not be added if either artifact_path is empty, or shutdown is True
    test_params['artifact_path'] = 's3://qa-sonic-enterprise-data-hub/artifacts/btyd-1.0.0/'
    test_params['shutdown'] = True
    handle_job_request(test_params, mock)

    # this API call_count will be 0 unless the job is added
    assert mock.list_running_cluster_instances.call_count == 0

    test_params['artifact_path'] = ''
    test_params['shutdown'] = False
    handle_job_request(test_params, mock)

    assert mock.list_running_cluster_instances.call_count == 0

    # job should be added under these parameters
    test_params['artifact_path'] = 's3://qa-sonic-enterprise-data-hub/artifacts/btyd-1.0.0/'
    test_params['shutdown'] = False
    handle_job_request(test_params, mock)

    assert mock.list_running_cluster_instances.call_count == 1



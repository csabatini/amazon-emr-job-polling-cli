from mock import Mock
from emr.job_client import handle_job_request

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
    "dryrun": True,
    "emr_version": "emr-5.6.0",
    "env": "qa",
    "h2o_backend": False,
    "job_args": "",
    "main_class": "com.sonicdrivein.datapipeline.Main",
    "project": "know_your_customer",
    "auto_terminate": True
}

mock_api = Mock()
mock_api.get_emr_cluster_with_name.return_value = [{'id': 'abc123'}]
mock_api.list_running_cluster_instances.return_value = {
    'Instances': [
        {'PrivateIpAddress': '192.168.0.1'},
        {'PrivateIpAddress': '192.168.0.2'},
        {'PrivateIpAddress': '192.168.0.3'}
    ]
}


def test_succeeds_with_default_params():
    output = mock_handle_job_request(params, mock_api)
    assert len(output) > 0
    assert output == "aws emr add-steps --profile qa --cluster-id abc123 --steps Type=Spark,Name=BTYD,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,--conf,'spark.app.name=BTYD',--conf,'spark.yarn.max.executor.failures=8',--conf,'spark.yarn.appMasterEnv.ENVIRONMENT=qa',--py-files,s3://qa-sonic-enterprise-data-hub/artifacts/btyd-1.0.0/application.zip,s3://qa-sonic-enterprise-data-hub/artifacts/btyd-1.0.0/main.py]"


def mock_handle_job_request(test_params, api):
    return handle_job_request(
        test_params, test_params['env'], test_params['job_name'], test_params['job_runtime'],
        test_params['job_timeout'], test_params['job_mode'], test_params['cluster_name'], test_params['artifact_path'],
        test_params['poll_cluster'], test_params['auto_terminate'], test_params['cicd'], test_params['airflow'],
        test_params['dryrun'], api
    )

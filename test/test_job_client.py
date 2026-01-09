from mock import Mock
from emr.job_client import handle_job_request
import pytest
import copy


class Context(object):
    def __init__(self, params={}):
        self.params = params


params = {
    "airflow": False,
    "cicd": False,
    "cluster_name": "Sandbox",
    "job_name": "BTYD",
    "job_runtime": "python",
    "job_timeout": 60,
    "poll_cluster": True,
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
mock_api.get_emr_cluster_with_name.return_value = []


# def test_succeeds_with_default_params():
#     output = mock_deploy(params, mock_api)
#     assert len(output) > 0
#     assert 'aws emr create-cluster' in output


def test_no_running_cluster_found_throws_error():
    with pytest.raises(AssertionError) as excinfo:
        mock_handle_job_request(params, mock_api)

    assert str(excinfo.value) == 'Expected 1 but found 0 running clusters with name Sandbox'


#
def mock_handle_job_request(test_params, api):
    ctx = Context(test_params)
    return handle_job_request(
        ctx, test_params['env'], test_params['job_name'], test_params['job_runtime'], test_params['job_timeout'],
        test_params['cluster_name'], test_params['artifact_path'], test_params['poll_cluster'],
        test_params['auto_terminate'], test_params['cicd'], test_params['airflow'], test_params['dryrun'], api
    )

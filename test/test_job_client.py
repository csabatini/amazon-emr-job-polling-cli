import textwrap
import pytest
from collections import namedtuple
from mock import Mock, call

from emr.job_client import handle_job_request


@pytest.fixture
def config():
    return {
        "airflow": False,
        "artifact_path": "s3://us-east-1.elasticmapreduce/samples/wordcount/",
        "checkpoint_bucket": "",
        "cluster_name": "Sandbox",
        "dryrun": True,
        "emr_version": "emr-5.6.0",
        "env": "qa",
        "job_args": "",
        "job_name": "WordCount",
        "job_runtime": "Python",
        "job_timeout": 60,
        "job_mode": "batch",
        "main_class": "org.apache.hadoop.tools.WordCount",
        "poll_cluster": False,
        "profile": "qa",
        "project": "",
        "shutdown": False,
        "terminate": True
    }


@pytest.fixture
def aws_api():
    mock_api = Mock()
    mock_api.get_emr_cluster_with_name.return_value = [{'id': 'cluster-123'}]
    mock_api.is_cluster_active.return_value = False
    mock_api.list_cluster_steps.return_value = ['step-123']
    mock_api.list_running_cluster_instances.return_value = {
        'Instances': [
            {'PrivateIpAddress': '192.168.0.1'},
            {'PrivateIpAddress': '192.168.0.2'}
        ]
    }
    return mock_api


@pytest.fixture
def grequests(mocker):
    Response = namedtuple('Response', ['status_code'])
    responses = [Response(200), Response(200)]

    mock_grequests = mocker.patch('emr.job_client.grequests', autospec=True)
    mock_grequests.post.return_value = None
    mock_grequests.get.return_value = None
    mock_grequests.map.return_value = responses
    return mock_grequests


def test_adds_expected_python_job(config, aws_api, grequests):
    expected_output = \
        """
        aws emr add-steps --profile qa --cluster-id cluster-123
         --steps Type=Spark,Name=WordCount,ActionOnFailure=CONTINUE,
        Args=[--deploy-mode,cluster,--master,yarn,--conf,
        'spark.app.name=WordCount',
        --conf,'spark.yarn.appMasterEnv.ENVIRONMENT=qa',--py-files,
        s3://us-east-1.elasticmapreduce/samples/wordcount/application.zip,
        s3://us-east-1.elasticmapreduce/samples/wordcount/main.py]
        """

    output = handle_job_request(config, aws_api)
    assert output
    assert textwrap.dedent(expected_output).replace('\n', '') == output


def test_invalid_job_runtime_throws_error(config, aws_api):
    with pytest.raises(ValueError) as excinfo:
        config['job_runtime'] = 'javascript'
        handle_job_request(config, aws_api)

    assert str(excinfo.value) == \
        "--job-runtime should be in ['scala', 'java', 'python']"


def test_shutdown_false_no_shutdown_api_calls(config, aws_api, grequests):
    config['job_runtime'] = 'Scala'
    handle_job_request(config, aws_api)

    # api.put_job_shutdown_marker not called
    assert aws_api.put_job_shutdown_marker.call_count == 0

    # api.add_checkpoint_copy_job not called
    assert aws_api.add_checkpoint_copy_job.call_count == 0


def test_shutdown_true_makes_shutdown_api_calls(config, aws_api, grequests):
    config['job_runtime'] = 'Scala'
    config['shutdown'] = True

    handle_job_request(config, aws_api)

    # api.put_job_shutdown_marker is called for this job
    calls = [call('qa-checkpoints', 'WordCount')]
    assert aws_api.put_job_shutdown_marker.call_count == 1
    aws_api.put_job_shutdown_marker.assert_has_calls(calls)

    # api.add_checkpoint_copy_job is called
    assert aws_api.add_checkpoint_copy_job.call_count == 1


def test_cluster_job_not_added_conditions(config, aws_api, grequests):
    # job should not be added if artifact_path is empty, or shutdown is True
    config['shutdown'] = True
    handle_job_request(config, aws_api)
    # TODO: assert on mock of run_cli_cmd
    assert aws_api.list_running_cluster_instances.call_count == 0

    config['artifact_path'] = ''
    config['shutdown'] = False
    handle_job_request(config, aws_api)
    # TODO: assert on mock of run_cli_cmd
    assert aws_api.list_running_cluster_instances.call_count == 0

    # job should be added under these parameters
    config['artifact_path'] = \
        's3://us-east-1.elasticmapreduce/samples/wordcount/'
    config['shutdown'] = False
    handle_job_request(config, aws_api)
    # TODO: assert on mock of run_cli_cmd
    assert aws_api.list_running_cluster_instances.call_count == 1

import textwrap
import pytest
from collections import namedtuple

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
def aws_api(mocker):
    mock_api = mocker.patch('emr.utils.AWSApi', autospec=True)
    mock_api.return_value.is_cluster_active.return_value = False
    mock_api.return_value.list_cluster_steps.return_value = ['step-123']
    mock_api.return_value.list_running_cluster_instances.return_value = {
        'Instances': [
            {'PrivateIpAddress': '192.168.0.1'},
            {'PrivateIpAddress': '192.168.0.2'}
        ]
    }

    cluster_info = [{'id': 'cl-359', 'name': 'TEST', 'state': 'RUNNING'}]
    mock_api.return_value.get_emr_cluster_with_name.return_value = cluster_info
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


@pytest.fixture
def shutdown_function(mocker):
    return mocker.patch('emr.job_client.shutdown_streaming_job')


@pytest.fixture
def shell_exec(mocker):
    return mocker.patch('emr.utils.run_shell_command', autospec=True)


def test_adds_expected_python_job(config, aws_api, grequests, shell_exec):
    expected_output = \
        """
        aws emr add-steps --profile qa --cluster-id cl-359
         --steps Type=Spark,Name=WordCount,ActionOnFailure=CONTINUE,
        Args=[--deploy-mode,cluster,--master,yarn,--conf,
        'spark.app.name=WordCount',
        --conf,'spark.yarn.appMasterEnv.ENVIRONMENT=qa',--py-files,
        s3://us-east-1.elasticmapreduce/samples/wordcount/application.zip,
        s3://us-east-1.elasticmapreduce/samples/wordcount/main.py]
        """

    output = handle_job_request(config)

    # TODO: aws cli should be invoked to submit the job
    # assert shell_exec.call_count == 1
    assert textwrap.dedent(expected_output).replace('\n', '') == output


def test_invalid_job_runtime_throws_error(config, aws_api, shell_exec):
    with pytest.raises(ValueError) as excinfo:
        config['job_runtime'] = 'javascript'
        handle_job_request(config)

    assert str(excinfo.value) == \
        "--job-runtime should be in ['scala', 'java', 'python']"


def test_shutdown_false_no_shutdown_api_calls(config,
                                              aws_api,
                                              grequests,
                                              shutdown_function,
                                              shell_exec):
    config['job_runtime'] = 'Scala'
    config['shutdown'] = False
    handle_job_request(config)

    assert not shutdown_function.called
    assert not aws_api.return_value.add_checkpoint_copy_job.called


def test_shutdown_true_makes_shutdown_api_calls(config,
                                                aws_api,
                                                grequests,
                                                shutdown_function,
                                                shell_exec):
    config['job_runtime'] = 'Scala'
    config['shutdown'] = True

    handle_job_request(config)

    assert shutdown_function.call_count == 1
    assert aws_api.return_value.add_checkpoint_copy_job.call_count == 1


def test_cluster_job_not_added_conditions(config,
                                          aws_api,
                                          grequests,
                                          shell_exec):
    # job should not be added if artifact_path is empty, or shutdown is True
    config['shutdown'] = True
    handle_job_request(config)
    # TODO: assert on mock of run_cli_cmd
    assert not aws_api.return_value.list_running_cluster_instances.called

    config['artifact_path'] = ''
    config['shutdown'] = False
    handle_job_request(config)
    # TODO: assert on mock of run_cli_cmd
    assert not aws_api.return_value.list_running_cluster_instances.called

    # job should be added under these parameters
    config['artifact_path'] = \
        's3://us-east-1.elasticmapreduce/samples/wordcount/'
    config['shutdown'] = False
    handle_job_request(config)
    # TODO: assert on mock of run_cli_cmd
    assert aws_api.return_value.list_running_cluster_instances.call_count == 1

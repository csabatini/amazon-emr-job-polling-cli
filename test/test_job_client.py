import copy
import textwrap
import pytest
import pytz
from collections import namedtuple
from datetime import datetime
from mock import call

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
def step_info():
    created_time = \
        datetime(2018, 1, 1, 0, 0, 0, 0).replace(tzinfo=pytz.utc)
    return [{
        'Id': 's-648',
        'Name': 'WordCount',
        'Status': {
            'State': 'RUNNING',
            'Timeline': {
                'CreationDateTime': created_time
            }
        }
    }]


@pytest.fixture
def aws_api(mocker, step_info):
    mock_api = mocker.patch('emr.utils.AWSApi', autospec=True)
    mock_api.return_value.is_cluster_active.return_value = False
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
def time_sleep(mocker):
    return mocker.patch('emr.job_client.time.sleep')


@pytest.fixture
def fixed_datetime(mocker):
    mock_dt = mocker.patch('emr.job_client.datetime.datetime')
    mock_dt.now.return_value = \
        datetime(2018, 1, 1, 0, 0, 0, 0).replace(tzinfo=pytz.utc)
    return mock_dt


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


def test_polls_until_job_state_completed(config,
                                         step_info,
                                         aws_api,
                                         grequests,
                                         time_sleep,
                                         fixed_datetime):
    config['poll_cluster'] = True
    job_response = copy.deepcopy(step_info[0])
    job_response['Status']['State'] = 'COMPLETED'

    # return two RUNNING states and then a COMPLETED state when called
    aws_api.return_value.list_cluster_steps.side_effect = \
        [step_info, step_info, [job_response]]

    handle_job_request(config)

    # expected time.sleep calls while polling job
    expected_calls = [call(60) for i in range(0, 3)]
    assert time_sleep.call_count == 3
    time_sleep.assert_has_calls(expected_calls)

    # expected interactions to list EMR steps
    expected_calls = [call('cl-359', 'WordCount', False) for i in range(0, 3)]
    assert aws_api.return_value.list_cluster_steps.call_count == 3
    aws_api.return_value.list_cluster_steps.assert_has_calls(expected_calls)


def test_polls_until_job_state_failed(config,
                                      step_info,
                                      aws_api,
                                      grequests,
                                      time_sleep,
                                      fixed_datetime):
    config['poll_cluster'] = True
    job_response = copy.deepcopy(step_info[0])
    job_response['Status']['State'] = 'FAILED'

    # return two RUNNING states and then a FAILED state when called
    aws_api.return_value.list_cluster_steps.side_effect = \
        [step_info, step_info, [job_response]]

    with pytest.raises(ValueError) as excinfo:
        handle_job_request(config)

    assert str(excinfo.value) == 'Job in invalid state FAILED'

    # expected time.sleep calls while polling job
    expected_calls = [call(60) for i in range(0, 3)]
    assert time_sleep.call_count == 3
    time_sleep.assert_has_calls(expected_calls)

    # expected interactions to list EMR steps
    expected_calls = [call('cl-359', 'WordCount', False) for i in range(0, 3)]
    assert aws_api.return_value.list_cluster_steps.call_count == 3
    aws_api.return_value.list_cluster_steps.assert_has_calls(expected_calls)


def test_polls_until_job_state_timeout(config,
                                       step_info,
                                       aws_api,
                                       grequests,
                                       time_sleep,
                                       fixed_datetime):
    config['poll_cluster'] = True
    job_response = copy.deepcopy(step_info[0])
    job_response['Status']['Timeline']['CreationDateTime'] = \
        datetime(2017, 12, 31, 0, 0, 0, 0).replace(tzinfo=pytz.utc)

    # return two RUNNING states and then an exceeded timeout when called
    aws_api.return_value.list_cluster_steps.side_effect = \
        [step_info, step_info, [job_response]]

    with pytest.raises(ValueError) as excinfo:
        handle_job_request(config)

    assert str(excinfo.value) == 'Job exceeded timeout 60'

    # expected time.sleep calls while polling job
    expected_calls = [call(60) for i in range(0, 3)]
    assert time_sleep.call_count == 3
    time_sleep.assert_has_calls(expected_calls)

    # expected interactions to list EMR steps
    expected_calls = [call('cl-359', 'WordCount', False) for i in range(0, 3)]
    assert aws_api.return_value.list_cluster_steps.call_count == 3
    aws_api.return_value.list_cluster_steps.assert_has_calls(expected_calls)


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

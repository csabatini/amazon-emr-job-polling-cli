import copy
import textwrap
import pytest
import pytz
from datetime import datetime
from mock import call

from emr.job_client import handle_job_request


@pytest.fixture
def config():
    return {
        "airflow": False,
        "artifact_path": "s3://us-east-1.elasticmapreduce/samples/wordcount/",
        "cluster_name": "Sandbox",
        "dryrun": False,
        "emr_version": "emr-5.6.0",
        "env": "qa",
        "job_args": "",
        "job_name": "WordCount",
        "job_runtime": "Python",
        "job_timeout": 60,
        "main_class": "org.apache.spark.examples.WordCount",
        "poll_cluster": False,
        "profile": "qa",
        "project": "",
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
def time_sleep(mocker):
    return mocker.patch('emr.job_client.time.sleep')


@pytest.fixture
def fixed_datetime(mocker):
    mock_dt = mocker.patch('emr.job_client.datetime.datetime')
    mock_dt.now.return_value = \
        datetime(2018, 1, 1, 0, 0, 0, 0).replace(tzinfo=pytz.utc)
    return mock_dt


@pytest.fixture
def shell_function(mocker):
    shell_fn = mocker.patch('emr.utils.run_shell_command', autospec=True)
    shell_fn.return_value = "{\"StepIds\": [\"s-F37BY4CL9\"]}"
    return shell_fn


def test_adds_expected_python_job(config, aws_api, shell_function):
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

    handle_job_request(config)

    shell_function.assert_called_once_with(
        textwrap.dedent(expected_output).replace('\n', ''))


def test_adds_expected_java_job(config, aws_api, shell_function):
    expected_output = \
        """
        aws emr add-steps --profile qa --cluster-id cl-359
         --steps Type=Spark,Name=WordCount,ActionOnFailure=CONTINUE,
        Args=[--deploy-mode,cluster,--master,yarn,--conf,
        'spark.app.name=WordCount',--class,org.apache.spark.examples.WordCount,
        --conf,'spark.driver.extraJavaOptions=-DenvironmentKey=qa',
        --conf,'spark.executor.extraJavaOptions=-DenvironmentKey=qa',
        s3://us-east-1.elasticmapreduce/samples/wordcount.jar,
        hdfs:///text-input/]
        """

    config['job_runtime'] = 'Java'
    config['artifact_path'] = \
        's3://us-east-1.elasticmapreduce/samples/wordcount.jar'
    config['job_args'] = 'hdfs:///text-input/'
    handle_job_request(config)

    shell_function.assert_called_once_with(
        textwrap.dedent(expected_output).replace('\n', ''))


def test_invalid_job_runtime_throws_error(config, aws_api, shell_function):
    with pytest.raises(ValueError) as excinfo:
        config['job_runtime'] = 'javascript'
        handle_job_request(config)

    assert str(excinfo.value) == \
        "--job-runtime should be in ['scala', 'java', 'python']"


def test_missing_output_resource_id_raises_error(config,
                                                 aws_api,
                                                 step_info,
                                                 time_sleep,
                                                 fixed_datetime,
                                                 shell_function):
    with pytest.raises(ValueError) as excinfo:
        shell_function.return_value = 'unexpected aws cli output'
        handle_job_request(config)

    assert str(excinfo.value) == 'StepIds not found in terminal output'


def test_polls_until_job_state_completed(config,
                                         step_info,
                                         aws_api,
                                         time_sleep,
                                         fixed_datetime,
                                         shell_function):
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
                                      time_sleep,
                                      fixed_datetime,
                                      shell_function):
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


def test_does_not_terminate_cluster_when_disabled(config,
                                                  step_info,
                                                  aws_api,
                                                  time_sleep,
                                                  fixed_datetime,
                                                  shell_function):
    step_info[0]['Status']['State'] = 'COMPLETED'
    aws_api.return_value.list_cluster_steps.return_value = step_info

    # set terminate to false
    config['poll_cluster'] = True
    config['terminate'] = False
    handle_job_request(config)

    assert not aws_api.return_value.terminate_clusters.called


def test_terminates_cluster_when_enabled(config,
                                         step_info,
                                         aws_api,
                                         time_sleep,
                                         fixed_datetime,
                                         shell_function):
    step_info[0]['Status']['State'] = 'COMPLETED'
    aws_api.return_value.list_cluster_steps.return_value = step_info

    # set terminate to true
    config['poll_cluster'] = True
    config['terminate'] = True
    handle_job_request(config)

    assert aws_api.return_value.terminate_clusters.call_count == 1


def test_polls_until_job_state_timeout(config,
                                       step_info,
                                       aws_api,
                                       time_sleep,
                                       fixed_datetime,
                                       shell_function):
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


def test_cluster_job_not_added_conditions(config,
                                          step_info,
                                          aws_api,
                                          time_sleep,
                                          shell_function):
    # job should not be added if artifact_path is empty
    config['artifact_path'] = ''
    handle_job_request(config)
    assert not shell_function.called

    # job should be added under these parameters
    config['artifact_path'] = \
        's3://us-east-1.elasticmapreduce/samples/wordcount/'
    handle_job_request(config)
    assert shell_function.call_count == 1

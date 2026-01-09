import os
import pytest
from mock import call, Mock
from os.path import dirname, join

from emr.utils import load_config, AWSApi

EXPECTED_LOG_HANDLERS = {
    'console': {
        'class': 'logging.StreamHandler',
        'level': 'DEBUG',
        'formatter': 'simple',
        'stream': 'ext://sys.stdout'
    }
}


@pytest.fixture
def list_clusters_response():
    return {
        'Clusters': [
            {'Id': '183', 'Name': 'EMR1', 'Status': {'State': 'RUNNING'}},
            {'Id': '359', 'Name': 'TEST', 'Status': {'State': 'RUNNING'}},
            {'Id': '637', 'Name': 'EMR3', 'Status': {'State': 'RUNNING'}}
        ]
    }


@pytest.fixture
def session(mocker, list_clusters_response):
    mock_s3 = Mock()
    mock_emr = Mock()
    mock_emr.list_clusters.return_value = list_clusters_response

    mock_session = mocker.patch('emr.utils.boto3.Session', autospec=True)
    mock_session.return_value.client.side_effect = [mock_s3, mock_emr]
    return mock_session


@pytest.fixture
def shell_exec(mocker):
    return mocker.patch('emr.utils.run_shell_command', autospec=True)


def test_s3_and_emr_clients_initialized(session):
    aws_api = AWSApi()
    expected_calls = [call('s3'), call('emr')]

    assert session.return_value.client.call_count == 2
    session.return_value.client.assert_has_calls(expected_calls)


def test_returns_empty_list_when_no_matching_clusters(session,
                                                      list_clusters_response):
    aws_api = AWSApi()
    list_clusters_response['Clusters'][1]['Name'] = 'EMR2'
    aws_api.emr.list_clusters.return_value = list_clusters_response

    # get clusters named TEST, of which there are none
    clusters = aws_api.get_emr_cluster_with_name('TEST')

    assert aws_api.emr.list_clusters.call_count == 1
    assert len(clusters) == 0


def test_returns_expected_matching_clusters(session):
    aws_api = AWSApi()

    # get clusters named TEST, of which there are none
    clusters = aws_api.get_emr_cluster_with_name('TEST')

    assert aws_api.emr.list_clusters.call_count == 1
    assert clusters == [{'id': '359', 'name': 'TEST', 'state': 'RUNNING'}]


def test_terminates_existing_matching_clusters(session, shell_exec):
    aws_api = AWSApi()
    expected_command = \
        'aws emr --profile qa terminate-clusters --cluster-id 359'

    aws_api.terminate_clusters('TEST', {'profile': 'qa'})

    # should list emr clusters to get the cluster id
    assert aws_api.emr.list_clusters.call_count == 1

    # should terminate cluster(s) with the aws cli
    shell_exec.assert_called_with(expected_command)


def test_loads_config_from_default_path():
    os.environ['LOG_CFG'] = ''
    result = load_config('logging.yml', 'LOG_CFG')

    assert isinstance(result, dict)
    assert 'handlers' in result
    assert result['handlers'] == EXPECTED_LOG_HANDLERS


def test_loads_config_from_env_variable():
    os.environ['LOG_CFG'] = join(dirname(__file__), 'dummy.yml')

    # env variable path should be loaded instead of config file in module
    result = load_config('logging.yml', 'LOG_CFG')

    assert isinstance(result, dict)
    assert 'handlers' not in result
    assert result == {'version': 1}


def test_load_failure_returns_empty_config():
    os.environ['LOG_CFG'] = ''
    result = load_config('missing.yml', 'LOG_CFG')

    assert isinstance(result, dict)
    assert 'handlers' not in result
    assert result == {}

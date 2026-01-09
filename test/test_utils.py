import os
from os.path import dirname, join

from emr.utils import load_config

EXPECTED_LOG_HANDLERS = {
    'console': {
        'class': 'logging.StreamHandler',
        'level': 'DEBUG',
        'formatter': 'simple',
        'stream': 'ext://sys.stdout'
    }
}


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

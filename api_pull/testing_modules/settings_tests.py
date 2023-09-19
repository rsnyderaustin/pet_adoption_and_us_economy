import pytest
import os

import settings
import testing_modules


@pytest.fixture(autouse=True)
def cleanup_after_test():
    settings.ConfigLoader.reset()
    settings.LogLoader.reset()


def get_toml_path():
    current_dir = os.path.dirname(testing_modules.__file__)
    return os.path.join(current_dir, 'test_variables.toml')


@pytest.fixture
def config_loader():
    return settings.ConfigLoader(get_toml_path())


@pytest.fixture
def log_loader():
    return settings.LogLoader(get_toml_path())


def test_get_config_success():
    config_value = settings.ConfigLoader.get_config(section='config_test',
                                                    config_name='test_config',
                                                    configs_file_path=get_toml_path())
    assert config_value == "Test config"


def test_get_config_fail():
    with pytest.raises(settings.configs.toml_config_loader.MissingConfigFileValueError):
        config_value = settings.ConfigLoader.get_config(section='bad_section',
                                                        config_name='bad_config',
                                                        configs_file_path=get_toml_path())


def test_get_message_success(log_loader):
    message = settings.LogLoader.get_log(section='logging_test',
                                         log_name='test_log',
                                         logs_file_path=get_toml_path())
    assert message == 'Test log'


def test_get_message_success_parameters(log_loader):
    message = settings.LogLoader.get_log(section='logging_test',
                                         log_name='test_log_parameters',
                                         parameters={
                                                 'parameter1': 'test one',
                                                 'parameter2': 'test two'
                                             }
                                         )
    assert message == 'Test log with parameters test one test two'


def test_get_message_fail(log_loader):
    with pytest.raises(settings.logs.toml_logging_messages_loader.MissingLogMessageError):
        message = settings.LogLoader.get_log(section='bad_section',
                                             log_name='bad_log_name')


def test_get_message_fail_parameters(log_loader):
    with pytest.raises(settings.logs.toml_logging_messages_loader.MissingLogMessageError):
        message = settings.LogLoader.get_log(section='logging_test',
                                             log_name='test_log_parameters',
                                             parameters={
                                                     'bad_parameter': 'bad',
                                                     'bad_parameter2': 'bad'
                                                 }
                                             )

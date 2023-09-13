import pytest

import configs

from ..configs import toml_config_loader


@pytest.fixture
def config_loader():
    config_loader = configs.ConfigLoader('test_variables.toml')


def test_get_config_success(config_loader):
    config_value = configs.ConfigLoader.get_config(section_name='config_test',
                                                   config_name='test_config')
    assert config_value == "Test config"


def test_get_config_fail(config_loader):
    with pytest.raises(toml_config_loader.MissingConfigFileValueError):
        config_value = configs.ConfigLoader.get_config(section_name='bad_section',
                                                       config_name='bad_config')

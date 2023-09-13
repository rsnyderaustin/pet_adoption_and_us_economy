import pytest

import configs


@pytest.fixture
def config_loader():
    config_loader = configs.ConfigLoader('C:/Users/austisnyder/Downloads/GitHub/pet_adoption_and_us_economy/api_pull/testing_modules/test_variables.toml')

@pytest.fixture
def log_loader():
    log_loader = configs.MsgLoader('C:/Users/austisnyder/Downloads/GitHub/pet_adoption_and_us_economy/api_pull/testing_modules/test_variables.toml')


def test_get_config_success(config_loader):
    config_value = configs.ConfigLoader.get_config(section_name='config_test',
                                                   config_name='test_config')
    assert config_value == "Test config"


def test_get_config_fail(config_loader):
    with pytest.raises(configs.toml_config_loader.MissingConfigFileValueError):
        config_value = configs.ConfigLoader.get_config(section_name='bad_section',
                                                       config_name='bad_config')

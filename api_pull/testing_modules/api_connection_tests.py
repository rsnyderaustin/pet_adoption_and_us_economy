import pytest
import tomli
import os
import requests
import requests_mock

from utils.petfinder import petfinder_api_connection_manager


@pytest.fixture
def toml_config_data():
    parent_package_path = os.path.dirname(os.path.dirname(__file__))
    config_file_path = os.path.join(parent_package_path, 'config.toml')
    with open(config_file_path, "rb") as toml_file:
        config_data = tomli.load(toml_file)
        return config_data


@pytest.fixture
def my_petfinder_api_connection_manager(toml_config_data):
    api_url = toml_config_data['petfinder_api']['api_url']
    token_url = toml_config_data['petfinder_api']['token_url']
    api_key = toml_config_data['petfinder_api']['api_key']
    secret_key = toml_config_data['petfinder_api']['secret_key']
    return petfinder_api_connection_manager.PetfinderApiConnectionManager(api_url=api_url,
                                                                          token_url=token_url,
                                                                          api_key=api_key,
                                                                          secret_key=secret_key)


@pytest.fixture
def my_fred_api_connection_manager(toml_config_data):
    api_url = toml_config_data['fred_api']['api_url']
    api_key = toml_config_data['fred_api']['api_key']
    return fred_api_connection_manager.FredApiConnectionManager(api_url=api_url,
                                                                api_key=api_key)


def test_petfinder_token_generator(my_petfinder_api_connection_manager):
    access_token = my_petfinder_api_connection_manager._generate_access_token()
    assert access_token is not None


def test_petfinder_animals_request_success(my_petfinder_api_connection_manager):
    test_category = 'animals'
    test_parameters = {
        'type': 'dog',
        'limit': 20
    }
    json_data = {"key1": "value1", "key2": "value2"}
    with requests_mock.Mocker() as mock:
        mock.get(my_petfinder_api_connection_manager.api_url, status_code=200, json=json_data)
        try:
            response_data = my_petfinder_api_connection_manager.make_request(category=test_category,
                                                                             parameters=test_parameters)
        except requests.exceptions.HTTPError as e:
            pytest.fail(f"HTTP Error raised with parameters {test_parameters}.")
    assert response_data == json_data, f"Expected JSON data {json_data}, received {response_data}"


def test_petfinder_organizations_request_success(my_petfinder_api_connection_manager):
    json_data = {"key1": "value1", "key2": "value2"}
    with requests_mock.Mocker() as mock:
        mock.get(my_petfinder_api_connection_manager.api_url, status_code=200, json=json_data)
        try:
            response_data = my_petfinder_api_connection_manager.make_request(category=test_category,
                                                                             parameters=test_parameters)
        except requests.exceptions.HTTPError as e:
            pytest.fail(f"HTTP Error raised with parameters {test_parameters}.")
    assert response_data == json_data, f"Expected JSON data {json_data}, received {response_data}"



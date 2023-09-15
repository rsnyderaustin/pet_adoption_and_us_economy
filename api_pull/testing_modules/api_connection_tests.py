import pytest
import tomli
import os
import requests
import requests_mock

from utils import PfManager, FredManager


@pytest.fixture
def toml_config_data():
    parent_package_path = os.path.dirname(os.path.dirname(__file__))
    config_file_path = os.path.join(parent_package_path, 'config.toml')
    with open(config_file_path, "rb") as toml_file:
        config_data = tomli.load(toml_file)
        return config_data


@pytest.fixture
def pf_manager(toml_config_data):
    api_url = toml_config_data['petfinder_api']['api_url']
    token_url = toml_config_data['petfinder_api']['token_url']
    api_key = toml_config_data['petfinder_api']['api_key']
    secret_key = toml_config_data['petfinder_api']['secret_key']
    return PfManager(api_url=api_url,
                     token_url=token_url,
                     api_key=api_key,
                     secret_key=secret_key)


@pytest.fixture
def FredManger(toml_config_data):
    api_url = toml_config_data['fred_api']['api_url']
    api_key = toml_config_data['fred_api']['api_key']
    return FredManager(api_url=api_url,
                       api_key=api_key)


def test_petfinder_token_generator(pf_manager):
    access_token = PfManager._generate_access_token()
    assert access_token is not None


def test_petfinder_animals_request_success(pf_manager):
    test_category = 'animals'
    test_parameters = {
        'type': 'dog',
        'limit': 20
    }
    json_data = {"key1": "value1", "key2": "value2"}
    with requests_mock.Mocker() as mock:
        mock.get(pf_manager.api_url, status_code=200, json=json_data)
        try:
            response_data = pf_manager.make_request(category=test_category,
                                                    parameters=test_parameters)
        except requests.exceptions.HTTPError as e:
            pytest.fail(f"HTTP Error raised with parameters {test_parameters}.")
    assert response_data == json_data, f"Expected JSON data {json_data}, received {response_data}"


def test_petfinder_organizations_request_success(pf_manager):
    json_data = {"key1": "value1", "key2": "value2"}
    with requests_mock.Mocker() as mock:
        mock.get(pf_manager.api_url, status_code=200, json=json_data)
        try:
            response_data = pf_manager.make_request(category=test_category,
                                                    parameters=test_parameters)
        except requests.exceptions.HTTPError as e:
            pytest.fail(f"HTTP Error raised with parameters {test_parameters}.")
    assert response_data == json_data, f"Expected JSON data {json_data}, received {response_data}"

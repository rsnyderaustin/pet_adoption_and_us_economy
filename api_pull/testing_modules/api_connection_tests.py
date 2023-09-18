import pytest
import tomli
import os
import requests
import requests_mock

from utils import PfManager, FredManager


@pytest.fixture
def toml_config_data():
    package_path = os.path.dirname(os.path.dirname(__file__))
    config_file_path = os.path.join(package_path, 'settings\configs\configs.toml')
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
def pf_token_mock():
    with requests_mock.mock() as m:
        m.post('https://api.petfinder.com/v2/oauth2/token', status_code=200, json={'access_token': '12345'})
        yield m


@pytest.fixture
def fred_manager(toml_config_data):
    api_url = toml_config_data['fred_api']['api_url']
    api_key = toml_config_data['fred_api']['api_key']
    return FredManager(api_url=api_url,
                       api_key=api_key)


def test_petfinder_animals_request_success(pf_manager, pf_token_mock):
    test_category = 'animals'
    test_parameters = {
        'type': 'dog',
        'limit': 20
    }
    json_data = {"key1": "value1", "key2": "value2"}
    api_url = pf_manager._generate_api_url(category=test_category)

    pf_token_mock.get(api_url, status_code=200, json=json_data)
    response = pf_manager.make_request(category=test_category,
                                            parameters=test_parameters)
    response_json = response.json()
    assert response_json == json_data, f"Expected JSON data {json_data}, received {response_json}"


def test_petfinder_organizations_request_success(pf_manager, pf_token_mock):
    test_category = 'organizations'
    test_parameters = {
        'state': 'IA'
    }
    json_data = {"key1": "value1", "key2": "value2"}
    api_url = pf_manager._generate_api_url(category=test_category)

    pf_token_mock.get(api_url, status_code=200, json=json_data)
    pf_token_mock.post(url='https://api.petfinder.com/v2/oauth2/token', status_code=200, json={'access_token': '12345'})
    response = pf_manager.make_request(category=test_category,
                                            parameters=test_parameters)
    response_json = response.json()
    assert response_json == json_data, f"Expected JSON data {json_data}, received {response_json}"


def test_get_last_date(fred_manager):
    json_data = {
        "realtime_start": "1776-07-04",
        "realtime_end": "9999-12-31",
        "order_by": "vintage_date",
        "sort_order": "asc",
        "count": 162,
        "offset": 0,
        "limit": 10000,
        "vintage_dates": [
            "1958-12-21",
            "1959-02-19",
            "1959-07-19",
            "1960-02-16",
            "1960-07-22",
            "1961-02-19",
            "1961-07-19",
            "1962-02-24",
            "1962-07-20",
            "1963-02-20",
            "1963-07-22",
            "1964-02-20",
            "1964-07-16",
            "1965-01-14",
            "1965-02-17",
            "1965-08-19",
            "1966-01-13"
        ]
    }

    with requests_mock.Mocker() as mock:
        api_url = fred_manager.generate_request_url(category='vintagedates')
        mock.get(url=api_url,
                 json=json_data)
        last_date = fred_manager.get_last_updated_date(tag='UNRATE')

        assert last_date == "1966-01-13"

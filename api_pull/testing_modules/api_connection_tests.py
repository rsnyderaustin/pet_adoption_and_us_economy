import os
import pytest
import requests_mock

from api_pull.settings import TomlConfigLoader, TomlLogsLoader
from api_pull.utils import PetfinderApiConnectionManager as PfManager
from api_pull.utils import FredApiConnectionManager as FredManager


@pytest.fixture
def toml_config_loader():
    return TomlConfigLoader()


@pytest.fixture
def pf_manager(toml_config_loader):
    api_url = toml_config_loader.get_config(section='petfinder_api', name='api_url')
    token_url = toml_config_loader.get_config(section='petfinder_api', name='token_url')
    api_key = toml_config_loader.get_config(section='petfinder_api', name='api_key')
    secret_key = toml_config_loader.get_config(section='petfinder_api', name='secret_key')
    return PfManager(api_url=api_url,
                     token_url=token_url,
                     api_key=api_key,
                     secret_key=secret_key)


@pytest.fixture
def fred_manager(toml_config_loader):
    api_url = toml_config_loader.get_config(section='fred_api', name='api_url')
    api_key = toml_config_loader.get_config(section='fred_api', name='api_key')
    return FredManager(api_url=api_url,
                       api_key=api_key)


@pytest.fixture
def pf_token_mock():
    with requests_mock.mock() as m:
        m.post('https://api.petfinder.com/v2/oauth2/token', status_code=200, json={'access_token': '12345',
                                                                                   'expires_in': 3600})
        yield m


def test_petfinder_animals_request_success(pf_manager, pf_token_mock):
    test_endpoint = 'animals'
    test_parameters = {
        'type': 'dog',
        'limit': 20
    }
    json_data = {"key1": "value1", "key2": "value2"}
    api_url = pf_manager._generate_api_url(path_endpoint=test_endpoint)

    pf_token_mock.get(api_url, status_code=200, json=json_data)
    response = pf_manager.make_request(path_endpoint=test_endpoint,
                                       parameters=test_parameters)
    response_json = response.json()
    assert response_json == json_data, f"Expected JSON data {json_data}, received {response_json}"


def test_petfinder_organizations_request_success(pf_manager, pf_token_mock):
    test_endpoint = 'organizations'
    test_parameters = {
        'state': 'IA'
    }
    json_data = {"key1": "value1", "key2": "value2"}
    api_url = pf_manager._generate_api_url(path_endpoint=test_endpoint)

    pf_token_mock.get(api_url, status_code=200, json=json_data)
    response = pf_manager.make_request(path_endpoint=test_endpoint,
                                       parameters=test_parameters)
    response_json = response.json()
    assert response_json == json_data, f"Expected JSON data {json_data}, received {response_json}"


def test_get_last_date_ordered(fred_manager):
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
        api_url = fred_manager._generate_api_url(path_segments='vintagedates')
        mock.get(url=api_url,
                 json=json_data)
        last_date = fred_manager.get_last_updated_date(tag='UNRATE')
        last_date_str = last_date.strftime('%Y-%m-%d')
        assert last_date_str == "1966-01-13"


def test_get_last_date_unordered(fred_manager):
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
            "1965-02-17",
            "1959-02-19",
            "1959-07-19",
            "1960-02-16",
            "1960-07-22",
            "1961-02-19",
            "1966-01-13",
            "1962-02-24",
            "1962-07-20",
            "1963-02-20",
            "1963-07-22",
            "1964-02-20",
            "1964-07-16",
            "1965-01-14",
            "1965-08-19",
            "1961-07-19"
        ]
    }

    with requests_mock.Mocker() as mock:
        api_url = fred_manager._generate_api_url(path_segments='vintagedates')
        mock.get(url=api_url,
                 json=json_data)
        last_date = fred_manager.get_last_updated_date(tag='UNRATE')
        last_date_str = last_date.strftime('%Y-%m-%d')
        assert last_date_str == "1966-01-13"

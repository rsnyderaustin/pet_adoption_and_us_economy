from datetime import datetime
import json
import os
import requests
from typing import Union
import urllib3
from urllib.parse import urljoin

from aws_lambda_powertools import Logger

from dynamodb_management import DynamoDbManager

from .petfinder_api_management import PetfinderApiConnectionManager as PfManager, PetfinderApiRequest as PfRequest

logger = Logger(service="petfinder_api_pull")

http = urllib3.PoolManager()

aws_session_token = os.environ['AWS_SESSION_TOKEN']
aws_region = os.environ['AWS_REGION']
cache_port = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
env = os.environ['ENV']
project_name = os.environ['FRED_PROJECT_NAME']


def retrieve_parameter_values(parameter_name):
    base_param_url = f'http://localhost:{cache_port}/systemsmanager/parameters/get?name='
    parameter_request_url = f'{base_param_url}%2F{env}%2f{project_name}/{parameter_name}'
    url = urljoin(base=base_param_url, url=parameter_request_url)

    param_secret_header = {"X-Aws-Parameters-Secrets-Token": aws_session_token}

    logger.info(f"Retrieving FRED config values from extension '{parameter_request_url}'.")
    response = http.request("GET", url, headers=param_secret_header)
    json_data = json.loads(response.data)

    return json_data


def determine_after_parameter(last_updated_day) -> datetime:



def create_pf_requests(requests_json) -> list[PfRequest]:
    pf_requests = []
    for request_name, request_values in requests_json.items():
        request_category = request_values['category']
        request_params = request_values.get('parameters', {})
        new_request = PfRequest(name=request_name,
                                category=request_category,
                                parameters=request_params)
        pf_requests.append(new_request)
    return pf_requests


def lambda_handler(event, context):
    raw_config_values = retrieve_parameter_values(parameter_name='configs')
    config_values = json.loads(raw_config_values)

    raw_pf_requests = retrieve_parameter_values(parameter_name='pf_requests')
    pf_requests_json = json.loads(raw_pf_requests)
    pf_requests = create_pf_requests(requests_json=pf_requests_json)

    dynamodb_manager = DynamoDbManager(table_name=config_values['db_table_name'],
                                       region=config_values['aws_region'],
                                       partition_key_name=config_values['db_partition_key_name'],
                                       sort_key_name=config_values['db_sort_key_name'])

    pf_manager = PfManager(api_url=config_values['pf_api_url'],
                           token_url=config_values['pf_token_url'])

    api_key = config_values['pf_api_key']
    secret_key = config_values['pf_secret_key']
    retry_seconds = config_values['pf_retry_seconds']
    access_token = PfManager.generate_access_token(api_key=api_key,
                                                   secret_key=secret_key,
                                                   retry_seconds=retry_seconds)

    for request in pf_requests:
        last_updated_day = dynamodb_manager.get_last_updated_day(partition_key_value=request.name,
                                                                 values_attribute_name=config_values['db_pf_values_attribute_name'])
        observation_start_str = determine_observation_start(last_updated_day=last_updated_day)

        if not observation_start_str:
            continue
        try:
            request_json_data = pf_manager.make_request(api_key=config_values['pf_api_key'],
                                                          pf_api_request=request,
                                                          observation_start=observation_start_str,
                                                          retry_seconds=config_values['pf_retry_seconds'])
            observations_data = request_json_data['observations']
            dynamodb_manager.put_pf_data(request_name=request.name,
                                           data=observations_data,
                                           values_attribute_name=config_values['db_pf_values_attribute_name'])
        except requests.exceptions.JSONDecodeError as e:
            logger.error(str(e))
            continue

def lambda_handler(event, context):
    pf_access_token = pf_manager.generate_access_token(api_key=pf_api_key,
                                                       secret_key=pf_secret_key)

    for request in pf_requests:
        request_json_data = pf_manager.make_request(access_token=pf_access_token,
                                                    petfinder_api_request=request,
                                                    max_tries=max_tries)
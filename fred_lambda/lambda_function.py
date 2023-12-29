from datetime import datetime
import json
import os
import requests
from typing import Union
import urllib3

from aws_lambda_powertools import Logger
from pet_adoption_and_us_economy.

from dynamodb_management import DynamoDbManager
from .fred_api_management import FredApiConnectionManager as FredManager, FredApiRequest as FredRequest


logger = Logger(service="fred_api_pull")

http = urllib3.PoolManager()

aws_session_token = os.environ['AWS_SESSION_TOKEN']
aws_region = os.environ['AWS_REGION']
cache_port = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
env = os.environ['ENV']
project_name = os.environ['FRED_PROJECT_NAME']


def determine_observation_start(last_updated_day: datetime) -> Union[str, None]:
    today_datetime = datetime.now()
    if last_updated_day == today_datetime:
        return None
    else:
        observation_start_str = last_updated_day.strftime('%Y-%m-%d')
        return observation_start_str


def create_fred_requests(requests_json) -> list[FredRequest]:
    fred_requests = []
    for request_name, request_values in requests_json.items():
        request_series_id = request_values['series_id']
        request_params = request_values.get('parameters', {})
        new_request = FredRequest(name=request_name,
                                  series_id=request_series_id,
                                  parameters=request_params)
        fred_requests.append(new_request)
    return fred_requests


def lambda_handler(event, context):
    raw_config_values = retrieve_parameter_values(parameter_name='configs')
    config_values = json.loads(raw_config_values)

    raw_fred_requests = retrieve_parameter_values(parameter_name='fred_requests')
    fred_requests_json = json.loads(raw_fred_requests)
    fred_requests = create_fred_requests(requests_json=fred_requests_json)

    dynamodb_manager = DynamoDbManager(table_name=config_values['db_table_name'],
                                       region=config_values['aws_region'],
                                       partition_key_name=config_values['db_partition_key_name'],
                                       sort_key_name=config_values['db_sort_key_name'])

    fred_manager = FredManager(api_url=config_values['fred_api_url'])

    for request in fred_requests:
        last_updated_day = dynamodb_manager.get_last_updated_day(partition_key_value=request.name,
                                                                 values_attribute_name=config_values['db_fred_values_attribute_name'])
        observation_start_str = determine_observation_start(last_updated_day=last_updated_day)

        if not observation_start_str:
            continue
        try:
            request_json_data = fred_manager.make_request(api_key=config_values['fred_api_key'],
                                                          fred_api_request=request,
                                                          observation_start=observation_start_str,
                                                          retry_seconds=config_values['fred_retry_seconds'])
            observations_data = request_json_data['observations']
            dynamodb_manager.put_fred_data(request_name=request.name,
                                           data=observations_data,
                                           values_attribute_name=config_values['db_fred_values_attribute_name'])
        except requests.exceptions.JSONDecodeError as e:
            logger.error(str(e))
            continue

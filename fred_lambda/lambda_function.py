from datetime import datetime, timedelta
import json
import os
import requests
from typing import Union
import urllib3

from aws_lambda_powertools import Logger
from aws_cache_retrieval import AwsVariableRetriever

from dynamodb_management import DynamoDbManager
from fred_lambda.fred_api_management import FredApiConnectionManager as FredManager, FredApiRequest as FredRequest


logger = Logger(service="fred_api_pull")

http = urllib3.PoolManager()

AWS_SESSION_TOKEN = os.environ['AWS_SESSION_TOKEN']
AWS_REGION = os.environ['AWS_REGION']
CACHE_PORT = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
ENV = os.environ['ENV']
PROJECT_NAME = os.environ['FRED_PROJECT_NAME']

aws_variable_retriever = AwsVariableRetriever(cache_port=CACHE_PORT,
                                              project_name=PROJECT_NAME,
                                              aws_session_token=AWS_SESSION_TOKEN)


def determine_observation_start(last_updated_day: datetime, default_date: str) -> Union[str, None]:
    """

    :param last_updated_day: datetime object representing the last updated day for the current request
    :param default_date: The default date for observation start of the current request. Only used when last_updated_day
        is None, indicating that there is no data for the current request.
    :return: The default date parameter if last_updated_day is None (indicating no data for the current request),
        or None if last_updated_day is today's date. Otherwise, returns the string representing the day after the last updated day.
    """
    # None value for last_updated_day indicates that there is no data for the current request
    if last_updated_day is None:
        return default_date
    today_datetime = datetime.now()
    if last_updated_day == today_datetime:
        return None
    else:
        day_after_last_update = last_updated_day + timedelta(days=1)
        observation_start_str = day_after_last_update.strftime('%Y-%m-%d')
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


@logger.inject_lambda_context
def lambda_handler(event, context):
    raw_config_values = aws_variable_retriever.retrieve_parameter_value(parameter_name='configs',
                                                                        expect_json=True)
    config_values = json.loads(raw_config_values)

    raw_fred_requests = aws_variable_retriever.retrieve_parameter_value(parameter_name='fred_requests',
                                                                        expect_json=True)
    fred_requests_json = json.loads(raw_fred_requests)
    fred_requests = create_fred_requests(requests_json=fred_requests_json)

    dynamodb_manager = DynamoDbManager(table_name=config_values['db_table_name'],
                                       region=AWS_REGION,
                                       partition_key_name=config_values['db_partition_key_name'],
                                       sort_key_name=config_values['db_sort_key_name'])

    fred_manager = FredManager(observations_api_url=config_values['fred_api_url'])

    default_data_start_date = config_values['default_data_start_date']
    for request in fred_requests:
        partition_key_value = f"fred_{request.name}"
        last_updated_day = dynamodb_manager.get_last_updated_day(partition_key_value=partition_key_value,
                                                                 values_attribute_name=config_values['db_fred_values_attribute_name'])
        observation_start_str = determine_observation_start(last_updated_day=last_updated_day,
                                                            default_date=default_data_start_date)

        if not observation_start_str:
            continue

        request.add_parameter(name='observation_start',
                              value=observation_start_str)
        try:
            request_json_data = fred_manager.make_request(api_key=config_values['fred_api_key'],
                                                          fred_api_request=request,
                                                          retry_seconds=config_values['fred_retry_seconds'])
            observations_data = request_json_data['observations']
            dynamodb_manager.put_fred_data(partition_key_value=request.name,
                                           data=observations_data,
                                           values_attribute_name=config_values['db_fred_values_attribute_name'])
            logger.info(f"Successfully uploaded data to DynamoDB for request {request.name}.")
        except requests.exceptions.JSONDecodeError as e:
            logger.error(str(e))
            continue

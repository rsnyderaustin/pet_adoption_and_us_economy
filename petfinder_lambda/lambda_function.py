from datetime import datetime, timedelta
import json
import os
import requests
from typing import Union
import urllib3

from aws_lambda_powertools import Logger

from aws_cache_retrieval import AwsVariableRetriever
from dynamodb_management import DynamoDbManager

from .petfinder_api_management import PetfinderApiConnectionManager as PfManager, PetfinderApiRequest as PfRequest

logger = Logger(service="petfinder_api_pull")

http = urllib3.PoolManager()

aws_session_token = os.environ['AWS_SESSION_TOKEN']
aws_region = os.environ['AWS_REGION']
cache_port = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
project_name = os.environ['FRED_PROJECT_NAME']

aws_variable_retriever = AwsVariableRetriever(cache_port=cache_port,
                                              project_name=project_name,
                                              aws_session_token=aws_session_token)


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


@logger.inject_lambda_context
def lambda_handler(event, context):
    raw_config_values = aws_variable_retriever.retrieve_parameter_value(parameter_name='configs',
                                                                        expect_json=True)
    config_values = json.loads(raw_config_values)

    raw_pf_requests = aws_variable_retriever.retrieve_parameter_value(parameter_name='pf_requests',
                                                                      expect_json=True)
    pf_requests_json = json.loads(raw_pf_requests)
    pf_requests = create_pf_requests(requests_json=pf_requests_json)

    dynamodb_manager = DynamoDbManager(table_name=config_values['db_table_name'],
                                       region=config_values['aws_region'],
                                       partition_key_name=config_values['db_partition_key_name'],
                                       sort_key_name=config_values['db_sort_key_name'])

    pf_manager = PfManager(api_url=config_values['pf_api_url'],
                           token_url=config_values['pf_token_url'])

    pf_access_token = aws_variable_retriever.retrieve_secret_value(secret_name='pf_access_token')

    dynamodb_manager = DynamoDbManager(table_name=config_values['db_table_name'],
                                       region=config_values['aws_region'],
                                       partition_key_name=config_values['db_partition_key_name'],
                                       sort_key_name=config_values['db_sort_key_name'])

    for request in pf_requests:
        last_updated_day = dynamodb_manager.get_last_updated_day(partition_key_value=request.name,
                                                                 values_attribute_name=config_values['db_pf_values_attribute_name'])
        request.add_parameter(name='after',
                              value=last_updated_day)

        try:
            request_json_data = pf_manager.make_request(access_token=pf_access_token,
                                                        petfinder_api_request=request,
                                                        retry_seconds=config_values['pf_retry_seconds'])
            observations_data = request_json_data['observations']
            partition_key_value = f"pf_{request.name}"
            dynamodb_manager.put_pf_data(data=observations_data,
                                         partition_key_value=partition_key_value,
                                         values_attribute_name=config_values['db_pf_values_attribute_name'])
        except requests.exceptions.JSONDecodeError as e:
            logger.error(str(e))
            continue
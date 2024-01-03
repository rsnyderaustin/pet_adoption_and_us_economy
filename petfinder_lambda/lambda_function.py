from datetime import datetime
import json
import os
import re
import requests
import urllib3

from aws_lambda_powertools import Logger

from aws_cache_retrieval import AwsVariableRetriever
from dynamodb_management import DynamoDbManager

from petfinder_lambda.petfinder_api_management import PetfinderApiConnectionManager as PfManager, \
    PetfinderApiRequest as PfRequest

logger = Logger(service="petfinder_api_pull")

http = urllib3.PoolManager()

AWS_SESSION_TOKEN = os.environ['AWS_SESSION_TOKEN']
AWS_REGION = os.environ['AWS_REGION']
CACHE_PORT = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
PROJECT_NAME = os.environ['FRED_PROJECT_NAME']

aws_variable_retriever = AwsVariableRetriever(cache_port=CACHE_PORT,
                                              project_name=PROJECT_NAME,
                                              aws_session_token=AWS_SESSION_TOKEN)


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


def published_at_str_into_datetime(published_at_str: str):
    date_match = re.match(r'([^T]+)T', published_at_str)
    date_string = date_match.group(1)
    logger.info(f"Animal date parsed to: '{date_string}'")
    datetime_obj = datetime.strptime(date_string, "%Y-%m-%d")
    return datetime_obj


def count_animals_by_date(json_data):
    """

    :param json_data:
    :return: Dict formatted: {"yyyy-mm" : {"dd" : value} }
    """
    logger.info(f"Processing JSON data in count_animals_by_date:\n{json_data}")
    dates_data = {}
    animals = json_data['animals']
    for animal in animals:
        published_at_string = animal['published_at']
        logger.info(f"Found animal date: '{published_at_string}'")

        datetime_obj = published_at_str_into_datetime(published_at_str=published_at_string)

        month_year = datetime_obj.strftime("%Y-%m")
        day = datetime_obj.strftime("%d")

        if month_year in dates_data:
            if day in dates_data[month_year]:
                dates_data[month_year] += 1
            else:
                dates_data[month_year][day] = 1
        else:
            dates_data[month_year] = 1
    return dates_data


@logger.inject_lambda_context
def lambda_handler(event, context):
    raw_config_values = aws_variable_retriever.retrieve_parameter_value(parameter_name='configs',
                                                                        expect_json=True)
    config_values = json.loads(raw_config_values)

    raw_pf_requests = aws_variable_retriever.retrieve_parameter_value(parameter_name='pf_requests',
                                                                      expect_json=True)
    pf_requests_json = json.loads(raw_pf_requests)
    pf_requests = create_pf_requests(requests_json=pf_requests_json)

    pf_access_token = aws_variable_retriever.retrieve_secret_value(secret_name='pf_access_token')

    pf_manager = PfManager(api_url=config_values['petfinder_api_url'],
                           access_token=pf_access_token)

    dynamodb_manager = DynamoDbManager(table_name=config_values['db_table_name'],
                                       region=AWS_REGION,
                                       partition_key_name=config_values['db_partition_key_name'],
                                       sort_key_name=config_values['db_sort_key_name'])

    for request in pf_requests:
        last_updated_month = dynamodb_manager.get_last_updated_month(partition_key_value=request.name,
                                                                     values_attribute_name=config_values[
                                                                         'db_pf_values_attribute_name']
                                                                     )

        request.add_parameter(name='after',
                              value=last_updated_month.strftime("%Y-%m-%d"))

        try:
            request_json_data = pf_manager.make_request(access_token=pf_access_token,
                                                        petfinder_api_request=request,
                                                        retry_seconds=config_values['pf_request_retry_seconds'])
            data_count = count_animals_by_date(json_data=request_json_data)

            dynamodb_manager.put_pf_data(data=data_count,
                                         partition_key_value=f"pf_{request.name}",
                                         values_attribute_name=config_values['db_pf_values_attribute_name'])
        except requests.exceptions.JSONDecodeError as e:
            logger.error(str(e))
            continue

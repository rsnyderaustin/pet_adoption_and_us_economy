import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
import json
import logging
import os
from typing import Union
import urllib3
from urllib3.exceptions import HTTPError

from api_pull import PetfinderApiConnectionManager as PfManager
from api_pull import FredApiConnectionManager as FredManager
from api_pull.utils import PetfinderApiRequest, FredApiRequest
from settings import TomlLogsLoader as LogsLoader

AWS_SESSION_TOKEN = os.environ['AWS_SESSION_TOKEN']
CACHE_PORT = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
PROJECT_NAME = os.environ['PARAMS_PROJECT_NAME']
HTTP = urllib3.PoolManager()
BASE_URL = f'http://localhost:{CACHE_PORT}/systemsmanager/parameters/get?name='
PETFINDER_LIFECYCLE_NAME = os.environ['PETFINDER_LIFECYCLE_NAME']
FRED_LIFECYCLE_NAME = os.environ['FRED_LIFECYCLE_NAME']


def retrieve_aws_parameter(env_variable_name, parameter_is_secret=False):
    """
    :param env_variable_name: The name of the environment variable storing the parameter name. A bit confusing,
        but it is formatted this way: 'environment variable -> AWS Parameter Store name -> [AWS Parameter Store value]'.
    :param parameter_is_secret: Whether the value stored in AWS Parameter Store is a SecureString.
    :return:
    """
    parameter_name = os.environ[env_variable_name].lower()
    request_url = f'{BASE_URL}%2F{PROJECT_NAME}%2f{parameter_name}/'

    if parameter_is_secret:
        headers = {"X-Aws-Parameters-Secrets-Token": os.environ.get(AWS_SESSION_TOKEN)}
        request_url += '&withDecryption=True'
        response = HTTP.request("GET", request_url, headers=headers)
    else:
        response = HTTP.request("GET", request_url)
    try:
        response.raise_for_status()
    except HTTPError as e:
        error_msg = LogsLoader.get_log(section='aws_lambda',
                                       log_name='parameter_request_http_error',
                                       parameters={'message': e})
        logging.error(error_msg)
        raise e
    except Exception as e:
        error_msg = LogsLoader.get_log(section='aws_lambda',
                                       log_name='parameter_request_other_error',
                                       parameters={'message': e})
        logging.error(error_msg)
        raise e

    try:
        value = response['Parameters'][0]['Value']
    except KeyError:
        error_msg = LogsLoader.get_log(section='aws_lambda',
                                       log_name='parameter_value_not_found')
        logging.error(error_msg)
        raise KeyError

    return value


def retrieve_api_request_configs(bucket_name, bucket_key) -> dict:
    """
    Returns a dict formatted:
        {

        'petfinder': [petfinder_api_requests],

        'fred': [fred_api_requests]

        }
    """
    # Retrieve file from S3
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name,
                                    Key=bucket_key)

    config_data = json.loads(response['Body'].read().decode('utf-8'))

    pf_request_configs = config_data[PETFINDER_LIFECYCLE_NAME]
    fred_request_configs = config_data[FRED_LIFECYCLE_NAME]

    pf_requests = []
    for api_request in pf_request_configs:
        request_name = api_request['name']
        request_category = api_request['category']
        request_params = api_request['parameters']
        new_request = PetfinderApiRequest(name=request_name,
                                          category=request_category,
                                          parameters=request_params)
        pf_requests.append(new_request)

    fred_requests = []
    for api_request in fred_request_configs:
        request_name = api_request['name']
        request_series_id = api_request['series_id']
        request_parameters = api_request['parameters']
        new_request = FredApiRequest(name=request_name,
                                     series_id=request_series_id,
                                     parameters=request_parameters)
        fred_requests.append(new_request)

    dict_to_return = {
        PETFINDER_LIFECYCLE_NAME: pf_requests,
        FRED_LIFECYCLE_NAME: fred_requests
    }

    return dict_to_return


def find_last_updated_day(dynamodb_table, partition_key_name, partition_key_value, sort_key_name, date_format: str) \
        -> Union[datetime, None]:
    """

    :param dynamodb_table:
    :param partition_key_name:
    :param partition_key_value:
    :param sort_key_name:
    :param date_today:
    :param date_format:
    :return: The date object of the last updated day in the DynamoDB table, or None if no date is found within 1000 days.
    """
    response = dynamodb_table.query(
        KeyConditionExpression="#pk = :pk AND #sk = :sk",
        ExpressionAttributeNames={
            '#pk': partition_key_name,
            '#sk': sort_key_name
        },
        ExpressionAttributeValues={
            ":pk": {'S': partition_key_value}
        },
        # Sort dates in descending order (most recent at the top)
        ScanIndexForward=False,
        Limit=1
    )
    if response and 'Items' in response:
        last_item = response['Items'][0]
        last_updated_day = last_item[sort_key_name]
        last_updated_day_object = datetime.strptime(last_updated_day, date_format)
        return last_updated_day_object
    else:
        return None


# Mandatory entry point for AWS Lambda
def lambda_handler(event, context):
    pf_api_url = retrieve_aws_parameter(env_variable_name='Needs updated')
    pf_access_token_url = retrieve_aws_parameter(env_variable_name='Needs updated')
    pf_manager = PfManager(api_url=pf_api_url,
                           token_url=pf_access_token_url)

    pf_api_key = retrieve_aws_parameter(env_variable_name='PETFINDER_API_KEY')
    pf_secret_key = retrieve_aws_parameter(env_variable_name='PETFINDER_SECRET_KEY')

    fred_api_url = retrieve_aws_parameter(env_variable_name='FRED_API_URL')
    fred_manager = FredManager(api_url=fred_api_url)
    fred_api_key = retrieve_aws_parameter(env_variable_name='FRED_API_KEY')

    request_configs_bucket_name = retrieve_aws_parameter(env_variable_name='REQUEST_CONFIGS_BUCKET_NAME')
    request_configs_bucket_key = retrieve_aws_parameter(env_variable_name='REQUEST_CONFIGS_BUCKET_KEY')
    api_requests = retrieve_api_request_configs(bucket_name=request_configs_bucket_name,
                                                bucket_key=request_configs_bucket_key)
    pf_requests = api_requests[PETFINDER_LIFECYCLE_NAME]
    fred_requests = api_requests[FRED_LIFECYCLE_NAME]

    pf_access_token = pf_manager.generate_access_token(api_key=pf_api_key,
                                                       secret_key=pf_secret_key)

    # Connect to DynamoDB
    dynamodb_client = boto3.resource('dynamodb')
    dynamodb_table_name = retrieve_aws_parameter(env_variable_name='DYNAMODB_TABLE_NAME')
    dynamodb_table = dynamodb_client.Table(dynamodb_table_name)

    dynamodb_partition_key = retrieve_aws_parameter(env_variable_name='DYNAMODB_PARTITION_KEY')
    dynamodb_sort_key = retrieve_aws_parameter(env_variable_name='DYNAMODB_SORT_KEY')

    date_today = datetime.now().date()
    date_format_param = retrieve_aws_parameter(env_variable_name='DATE_STRING_FORMAT')

    for request in pf_requests:
        request_name = request.name
        request_json_data = pf_manager.make_request(access_token=pf_access_token,
                                                    petfinder_api_request=request)

        # Push JSON data to dynamoDB big data table, and update


    fred_api_max_retries = retrieve_aws_parameter(env_variable_name='FRED_API_MAX_RETRIES')
    fred_api_request_retry_delay = retrieve_aws_parameter(env_variable_name='FRED_API_REQUEST_RETRY_DELAY')

    for request in fred_requests:
        request_name = request.name
        last_updated_day = find_last_updated_day(dynamodb_table=dynamodb_table,
                                                 partition_key_name=dynamodb_partition_key,
                                                 partition_key_value=request_name,
                                                 sort_key_name=dynamodb_sort_key,
                                                 date_today=date_today,
                                                 date_format=date_format_param)

        # We are only looking to get a snapshot of the data once per day, so skip the series if we already have today's
        # data
        if last_updated_day == date_today:
            log_msg = LogsLoader.get_log(section='aws_lambda',
                                         log_name='have_todays_data',
                                         parameters={'series_name': request_name})
            logging.info(log_msg)
            continue
        elif last_updated_day is None:
            # Last updated day is None when no day is found to have been updated in the last 2 years. Meaning there
            observation_start_str = '1776-07-04'

        # We want to get the data after the last updated day
        observation_start = last_updated_day + timedelta(days=1)
        observation_start_str = observation_start.strftime(date_format_param)

        request_json_data = fred_manager.make_request(api_key=fred_api_key,
                                                      fred_api_request=request,
                                                      observation_start=observation_start_str,
                                                      max_retries=fred_api_max_retries,
                                                      retry_delay=fred_api_request_retry_delay)

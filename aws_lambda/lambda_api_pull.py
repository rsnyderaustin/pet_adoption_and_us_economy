import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
import json
import logging
import os
from typing import Union
import urllib3
from urllib3.exceptions import HTTPError

from aws_lambda_powertools import Logger


def retrieve_aws_parameter(env_variable_name, parameter_is_secret=False):
    """
    :param env_variable_name: The name of the environment variable storing the parameter name. A bit confusing,
        but it is formatted this way: 'environment variable -> AWS Parameter Store name -> [AWS Parameter Store value]'.
    :param parameter_is_secret: Whether the value stored in AWS Parameter Store is a SecureString.
    :return:
    """
    parameter_name = os.environ[env_variable_name].lower()
    request_url = f'{base_param_url}%2F{project_name}%2f{parameter_name}/'

    if parameter_is_secret:
        headers = {"X-Aws-Parameters-Secrets-Token": os.environ.get(aws_session_token)}
        request_url += '&withDecryption=True'
        response = http.request("GET", request_url, headers=headers)
    else:
        response = http.request("GET", request_url)

    response.raise_for_status()

    value = response['Parameters'][0]['Value']

    return value


def retrieve_api_request_configs(bucket_name, bucket_key) -> dict:
    """
    Returns a dict formatted:
        {

        'petfinder_api_management': [petfinder_api_requests],

        'fred_api_management': [fred_api_requests]

        }
    """
    # Retrieve file from S3
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name,
                                    Key=bucket_key)

    config_data = json.loads(response['Body'].read().decode('utf-8'))

    pf_request_configs = config_data[petfinder_lifecycle_name]
    fred_request_configs = config_data[fred_lifecycle_name]

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
        petfinder_lifecycle_name: pf_requests,
        fred_lifecycle_name: fred_requests
    }

    return dict_to_return

aws_session_token = os.environ['AWS_SESSION_TOKEN']
aws_region = os.environ['AWS_REGION']
cache_port = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
project_name = os.environ['PARAMS_PROJECT_NAME']
http = urllib3.PoolManager()
base_param_url = f'http://localhost:{cache_port}/systemsmanager/parameters/get?name='
petfinder_lifecycle_name = os.environ['PETFINDER_LIFECYCLE_NAME']
fred_lifecycle_name = os.environ['FRED_LIFECYCLE_NAME']

fred_api_url = retrieve_aws_parameter(env_variable_name='FRED_API_URL')
fred_manager = FredManager(api_url=fred_api_url)

fred_api_key = retrieve_aws_parameter(env_variable_name='FRED_API_KEY')

fred_api_max_retries = retrieve_aws_parameter(env_variable_name='FRED_API_MAX_RETRIES')
fred_api_request_retry_delay = retrieve_aws_parameter(env_variable_name='FRED_API_REQUEST_RETRY_DELAY')
fred_output_date_format = retrieve_aws_parameter(env_variable_name='FRED_API_OUTPUT_DATE_FORMAT')

dynamodb_table_name = retrieve_aws_parameter(env_variable_name='DYNAMODB_TABLE_NAME')
dynamodb_partition_key = retrieve_aws_parameter(env_variable_name='DYNAMODB_PARTITION_KEY')
dynamodb_sort_key = retrieve_aws_parameter(env_variable_name='DYNAMODB_SORT_KEY')
dynamodb_date_format = retrieve_aws_parameter(env_variable_name='DYNAMODB_DATE_STRING_FORMAT')

dynamodb_handler = DynamoDbHandler(table_name=dynamodb_table_name,
                                   region=aws_region,
                                   partition_key_name=dynamodb_partition_key,
                                   sort_key_name=dynamodb_sort_key,
                                   sort_key_date_format=dynamodb_date_format)

# Get the configurations for each API request from a JSON file in an AWS S3 bucket
# See function doc for return format
request_configs_bucket_name = retrieve_aws_parameter(env_variable_name='REQUEST_CONFIGS_BUCKET_NAME')
request_configs_bucket_key = retrieve_aws_parameter(env_variable_name='REQUEST_CONFIGS_BUCKET_KEY')
api_requests = retrieve_api_request_configs(bucket_name=request_configs_bucket_name,
                                            bucket_key=request_configs_bucket_key)

pf_requests = api_requests[petfinder_lifecycle_name]
fred_requests = api_requests[fred_lifecycle_name]

date_today = datetime.now().date()

def determine_fred_observation_start(last_updated_day, request_name) -> Union[str, None]:
    """

    :param last_updated_day:
    :param request_name:
    :return: The string representing the 'observation_start' parameter for a FRED API request, or None if
        the data is up to date for the current date.
    """
    def last_updated_day_is_today(date_today, last_updated_day):
        return date_today == last_updated_day

    def no_dates_found_for_dataset(last_updated_day):
        return last_updated_day is None

    def day_after_last_updated_day(last_updated_day, fred_date_format):
        day_after = last_updated_day + timedelta(days=1)
        observation_start_str = day_after.strftime(fred_date_format)
        return observation_start_str

    # Skip the series if we already have today's data
    if last_updated_day_is_today(last_updated_day, date_today):
        log_msg = LogsLoader.get_log(section='aws_lambda',
                                     log_name='have_todays_data',
                                     parameters={'series_name': request_name})
        logging.info(log_msg)
        return None
    elif no_dates_found_for_dataset(last_updated_day):
        log_msg = LogsLoader.get_log(section='aws_lambda',
                                     log_name='no_dynamodb_data_found',
                                     parameters={'series_name': request_name})
        logging.info(log_msg)
        # Set default observation start to Jan 1st, 2000 - this value will change once it's determined how far back
        # Petfinder data goes
        return '2000-01-01'
    else:
        observation_start_str = day_after_last_updated_day(last_updated_day, fred_output_date_format)
        return observation_start_str



# Mandatory entry point for AWS Lambda
def lambda_handler(event, context):
    pf_access_token = pf_manager.generate_access_token(api_key=pf_api_key,
                                                       secret_key=pf_secret_key)

    for request in pf_requests:
        request_json_data = pf_manager.make_request(access_token=pf_access_token,
                                                    petfinder_api_request=request)

        # Push JSON data to dynamoDB big data table, and update

    for request in fred_requests:
        last_updated_day = dynamodb_handler.get_last_updated_day(partition_key_value=request.name)
        observation_start_str = determine_fred_observation_start(last_updated_day=last_updated_day,
                                                                 request_name=request.name)
        if not observation_start_str:
            continue
        try:
            request_json_data = fred_manager.make_request(api_key=fred_api_key,
                                                          fred_api_request=request,
                                                          observation_start=observation_start_str,
                                                          max_retries=fred_api_max_retries,
                                                          retry_delay=fred_api_request_retry_delay)
            observations_data = request_json_data['observations']
            dynamodb_handler.put_fred_data(request_name=request.name,
                                           data=observations_data)
            for observation in request_json_data['observations']:
                date = observation['date']
                value = observation['value']
                dynamodb_handler.put_

        except Exception as e:
            logging.error(str(e))
            continue

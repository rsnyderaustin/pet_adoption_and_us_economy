import boto3
from datetime import datetime
import json
import logging
import os
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


def retrieve_aws_parameter(env_variable_name, secret=False):
    """
    :param env_variable_name: The name of the environment variable storing the parameter name. A bit confusing,
        but it is formatted like this 'environment variable -> AWS Parameter Store name -> [AWS Parameter Store value]'.
    :param secret: Whether the value stored in AWS Parameter Store is a SecureString.
    :return:
    """
    env_variable_name = os.environ[env_variable_name]
    request_url = f'{BASE_URL}%2F{PROJECT_NAME}%2f{env_variable_name}/'

    if secret:
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
    s3_client = boto3.client('s3')
    bucket_name = retrieve_aws_parameter(env_variable_name='###')
    api_request_configs_key = retrieve_aws_parameter(env_variable_name='###')
    response = s3_client.get_object(Bucket=bucket_name,
                                    Key=api_request_configs_key)
    # API request config data is stored as a list of dicts
    config_data = json.loads(response['Body'].read().decode('utf-8'))

    pf_request_configs = config_data['petfinder']
    fred_request_configs = config_data['fred']

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
        'petfinder': pf_requests,
        'fred': fred_requests
    }

    return dict_to_return


# Mandatory entry point for AWS Lambda
def lambda_handler(event, context):
    pf_api_url = retrieve_aws_parameter(env_variable_name='Needs updated')
    pf_access_token_url = retrieve_aws_parameter(env_variable_name='Needs updated')
    pf_manager = PfManager(api_url=pf_api_url,
                           token_url=pf_access_token_url)

    pf_api_key = retrieve_aws_parameter(env_variable_name='Needs updated')
    pf_secret_key = retrieve_aws_parameter(env_variable_name='Needs updated')

    fred_api_url = retrieve_aws_parameter(env_variable_name='Needs updated')
    fred_manager = FredManager(api_url=fred_api_url)
    fred_key = retrieve_aws_parameter(env_variable_name='Needs updated')

    configs_bucket_name = retrieve_aws_parameter(env_variable_name='###')
    configs_bucket_key = retrieve_aws_parameter(env_variable_name='###')
    api_requests = retrieve_api_request_configs(bucket_name=configs_bucket_name,
                                                bucket_key=configs_bucket_key)
    pf_lifecycle_name = retrieve_aws_parameter(env_variable_name='Needs updated')
    pf_requests = api_requests[pf_lifecycle_name]
    fred_lifecycle_name = retrieve_aws_parameter(env_variable_name='Needs updated')
    fred_requests = api_requests[fred_lifecycle_name]

    pf_access_token = pf_manager.generate_access_token(api_key=pf_api_key,
                                                       secret_key=pf_secret_key)

    dynamodb_client = boto3.resource('dynamodb')
    dynamodb_meta_table_name = retrieve_aws_parameter(env_variable_name='Needs updated')
    meta_table = dynamodb_client.Table(dynamodb_meta_table_name)
    for request in pf_requests:
        request_name = request.name
        request_name_col = retrieve_aws_parameter(env_variable_name='Needs updated')
        dynamodb_response = meta_table.query(
            KeyConditionExpress=Key(request_name_col).eq(request_name)
        )
        last_updated_col = retrieve_aws_parameter(env_variable_name='Needs updated')
        last_updated = dynamodb_response[last_updated_col]
        request_json_data = pf_manager.make_request(access_token=pf_access_token,
                                                    petfinder_api_request=request)

        # Push JSON data to dynamoDB big data table, and update

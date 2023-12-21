import boto3
import json
import os
import requests
import urllib3

from aws_lambda_powertools import Logger

from .fred_api_management.fred_api_connection_manager import FredApiConnectionManager as FredManager
from .fred_api_management.fred_api_request import FredApiRequest as FredRequest
from

logger = Logger(service="fred_api_pull")


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
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error(str(e))
        raise e
    except Exception as e:
        logger.error(str(e))

    try:
        value = response['Parameters'][0]['Value']
    except KeyError as e:
        logger.error(str(e))
        raise KeyError

    return value


def retrieve_request_configs(bucket_name, bucket_key, type: str) -> list[FredRequest]:
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

    fred_request_configs = config_data[fred_lifecycle_name]

    fred_requests = []
    for api_request in fred_request_configs:
        request_name = api_request['name']
        request_series_id = api_request['series_id']
        request_parameters = api_request['parameters']
        new_request = FredRequest(name=request_name,
                                  series_id=request_series_id,
                                  parameters=request_parameters)
        fred_requests.append(new_request)

    return fred_requests


aws_session_token = os.environ['AWS_SESSION_TOKEN']
aws_region = os.environ['AWS_REGION']
cache_port = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
project_name = os.environ['PARAMS_PROJECT_NAME']
http = urllib3.PoolManager()
base_param_url = f'http://localhost:{cache_port}/systemsmanager/parameters/get?name='
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

fred_requests = retrieve_request_configs()

def lambda_handler(event, context):
    for request in fred_requests:
        last_updated_day = dynamodb_manager.get_last_updated_day(partition_key_value=request.name)
        observation_start_str = determine_observation_start(last_updated_day=last_updated_day,
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
            dynamodb_manager.put_fred_data(request_name=request.name,
                                           data=observations_data)
            for observation in request_json_data['observations']:
                date = observation['date']
                value = observation['value']
                dynamodb_manager.put_

        except Exception as e:
            logging.error(str(e))
            continue

import boto3
import json
import os
import urllib3
import requests
from aws_lambda_powertools import Logger

from .petfinder_api_management.petfinder_api_connection_manager import PetfinderApiConnectionManager as PfManager
from .petfinder_api_management.petfinder_api_request import PetfinderApiRequest as PfRequest

logger = Logger(service="petfinder_api_pull")

aws_session_token = os.environ['AWS_SESSION_TOKEN']
aws_region = os.environ['AWS_REGION']
cache_port = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
project_name = os.environ['PARAMS_PROJECT_NAME']
http = urllib3.PoolManager()
base_param_url = f'http://localhost:{cache_port}/systemsmanager/parameters/get?name='
petfinder_lifecycle_name = os.environ['PETFINDER_LIFECYCLE_NAME']
fred_lifecycle_name = os.environ['FRED_LIFECYCLE_NAME']


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


def retrieve_request_configs(bucket_name, bucket_key) -> list[PfRequest]:
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
        new_request = PfRequest(name=request_name,
                                          category=request_category,
                                          parameters=request_params)
        pf_requests.append(new_request)

    return pf_requests

pf_api_url = retrieve_aws_parameter(env_variable_name='PETFINDER_API_URL')
pf_access_token_url = retrieve_aws_parameter(env_variable_name='PETFINDER_ACCESS_TOKEN_URL')
pf_manager = PfManager(api_url=pf_api_url,
                       token_url=pf_access_token_url)

pf_api_key = retrieve_aws_parameter(env_variable_name='PETFINDER_API_KEY')
pf_secret_key = retrieve_aws_parameter(env_variable_name='PETFINDER_SECRET_KEY')

dynamodb_table_name = retrieve_aws_parameter(env_variable_name='DYNAMODB_TABLE_NAME')
dynamodb_partition_key = retrieve_aws_parameter(env_variable_name='DYNAMODB_PARTITION_KEY')
dynamodb_sort_key = retrieve_aws_parameter(env_variable_name='DYNAMODB_SORT_KEY')
dynamodb_date_format = retrieve_aws_parameter(env_variable_name='DYNAMODB_DATE_STRING_FORMAT')

request_configs_bucket_name = retrieve_aws_parameter(env_variable_name='REQUEST_CONFIGS_BUCKET_NAME')
request_configs_bucket_key = retrieve_aws_parameter(env_variable_name='REQUEST_CONFIGS_BUCKET_KEY')

api_requests = retrieve_request_configs(bucket_name=request_configs_bucket_name,
                                        bucket_key=request_configs_bucket_key)

pf_requests = api_requests[petfinder_lifecycle_name]

def lambda_handler(event, context):
    pf_access_token = pf_manager.generate_access_token(api_key=pf_api_key,
                                                       secret_key=pf_secret_key)

    for request in pf_requests:
        request_json_data = pf_manager.make_request(access_token=pf_access_token,
                                                    petfinder_api_request=request)
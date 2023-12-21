import boto3
import http
import json
import os
import requests

from aws_lambda_powertools import Logger

aws_session_token = os.environ['AWS_SESSION_TOKEN']
cache_port = os.environ['PARAMETERS_SECRETS_ETENSION_HTTP_PORT']
base_param_url = f'http://localhost:{cache_port}/systemsmanager/parameters/get?name='
project_name = os.environ['PARAMETERS_PROJECT_NAME']

petfinder_lifecycle_name = os.environ['PETFINDER_LIFECYCLE_NAME']
fred_lifecycle_name = os.environ['FRED_LIFECYCLE_NAME']

logger = Logger(case="aws_retrieval_functions")

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

def get_petfinder_configs() ->

def retrieve_api_request_configs(which_api: str, bucket_name, bucket_key) -> dict:
    """
    Returns a dict formatted:
        {

        'petfinder_api_management': [petfinder_api_requests],

        'fred_api_management': [fred_api_requests]

        }
    :param which_api: Which of 'petfinder' or 'fred' configs to return
    """
    # Retrieve file from S3
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name,
                                    Key=bucket_key)

    config_data = json.loads(response['Body'].read().decode('utf-8'))

    which_api = which_api.lower()
    if which_api in 'petfinder':
        petfinder_lifecycle_name = os.environ['PETFINDER_LIFECYCLE_NAME']
        pf_request_configs = config_data[petfinder_lifecycle_name]
    elif which_api in 'fred':
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

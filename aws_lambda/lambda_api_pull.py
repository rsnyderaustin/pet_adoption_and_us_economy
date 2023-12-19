import json
import logging
import os
import urllib3
from urllib3.exceptions import HTTPError

from api_pull import PetfinderApiConnectionManager as PfManager
from api_pull import FredApiConnectionManager as FredManager
from settings import TomlLogsLoader as LogsLoader

AWS_SESSION_TOKEN = os.environ['AWS_SESSION_TOKEN']
CACHE_PORT = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
PROJECT_NAME = os.environ['PARAMS_PROJECT_NAME']
HTTP = urllib3.PoolManager()
BASE_URL = f'http://localhost:{CACHE_PORT}/systemsmanager/parameters/get?name='


def retrieve_aws_parameter(parameter_name, secret=False):
    request_url = f'{BASE_URL}%2F{PROJECT_NAME}%2f{parameter_name}/'

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


# Mandatory entry point for AWS Lambda
def lambda_handler(event, context):
    pf_api_url = retrieve_aws_parameter(parameter_name='Needs updated')
    pf_access_token_url = retrieve_aws_parameter(parameter_name='Needs updated')
    pf_manager = PfManager(api_url=pf_api_url,
                           token_url=pf_access_token_url)
    pf_key = retrieve_aws_parameter(parameter_name='Needs updated')
    pf_secret_key = retrieve_aws_parameter(parameter_name='Needs updated')
    pf_access_token = pf_manager.generate_access_token(api_key=pf_key,
                                                       secret_key=pf_secret_key)

    fred_api_url = retrieve_aws_parameter(parameter_name='Needs updated')
    fred_manager = FredManager(api_url=fred_api_url)
    fred_key = retrieve_aws_parameter(parameter_name='Needs updated')

    date_of_last_update = retrieve_aws_parameter(parameter_name='Needs updated')

    # Need to figure out how to format and store individual requests
    pf_dog_data = pf_manager.make_request(path_endpoint='animals',
                                          parameters={'type': 'dog',
                                                      'status': 'adoptable'})
    pf_cat_data = pf_manager.make_request(path_endpoint='animals',
                                          parameters={'type': 'cat',
                                                      'status': 'adoptable'})

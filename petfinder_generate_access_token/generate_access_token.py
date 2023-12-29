from json.decoder import JSONDecodeError
from aws_lambda_powertools import Logger
import http
import json
import os
import requests
from urllib.parse import urljoin

logger = Logger(service="petfinder_access_token_generator")


aws_session_token = os.environ['AWS_SESSION_TOKEN']
aws_region = os.environ['AWS_REGION']
cache_port = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
project_name = os.environ['FRED_PROJECT_NAME']


def retrieve_parameter_values(parameter_name, expect_json=True):
    base_param_url = f'http://localhost:{cache_port}/systemsmanager/parameters/get?name='
    parameter_request_url = f'{base_param_url}%2f{project_name}%2f{parameter_name}'
    url = urljoin(base=base_param_url, url=parameter_request_url)

    header = {"X-Aws-Parameters-Secrets-Token": aws_session_token}

    logger.info(f"Retrieving Petfinder AWS Parameter values from extension '{parameter_request_url}'.")
    response = http.request("GET", url, headers=header)
    if expect_json:
        json_data = json.loads(response.data)
        return json_data
    else:
        parameter_value = response.data.decode('utf-8')
        return parameter_value

def retrieve_secret_value(secret_name, expect_json=True):
    base_secret_url = f'http://localhost:{cache_port}/secretsmanager/get?secretId='
    secret_request_url = f'{base_secret_url}%2F{project_name}%2f{secret_name}'
    url = urljoin(base=base_secret_url, url=secret_request_url)

    header = {"X-Aws-Parameters-Secrets-Token": aws_session_token}

    logger.info(f"Retrieving Petfinder AWS Secret value from extension '{secret_request_url}'.")
    response = http.request("GET", url, headers=header)
    secret_value = response.data.decode('utf-8')
    return secret_value



"""
Generates a new Petfinder access token if necessary, and returns a valid token.
:return: Petfinder API access token.
"""

data = {
    'grant_type': 'client_credentials',
    'client_id': api_key,
    'client_secret': secret_key
}
max_tries = len(retry_seconds) - 1
for tries in range(max_tries):
    if tries >= 1:
        self.logger.info(f"Retry number {tries} for generating a Petfinder access token.")
    try:
        response = requests.post(url=self.token_url, data=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        self.logger.error(str(e))
        return e
    try:
        response_data = response.json()
    except JSONDecodeError as e:
        self.logger.error(str(e))
        return e
    try:
        return response_data['access_token']
    except KeyError as e:
        self.logger.error(str(e))
        raise e
self.logger.error(f"Max number of tries ({max_tries}) reached when generating Petfinder access token.")
raise MaxGenerateAccessTokenTriesError
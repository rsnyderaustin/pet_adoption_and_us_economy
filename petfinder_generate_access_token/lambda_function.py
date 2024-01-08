import json
import os
import requests

from aws_lambda_powertools import Logger

from aws_cache_retrieval import AwsVariableRetriever

logger = Logger(service="pf_access_token_generator")


AWS_SESSION_TOKEN = os.environ['AWS_SESSION_TOKEN']
AWS_REGION = os.environ['AWS_REGION']
CACHE_PORT = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
PROJECT_NAME = os.environ['FRED_PROJECT_NAME']

aws_variable_retriever = AwsVariableRetriever(cache_port=CACHE_PORT,
                                              project_name=PROJECT_NAME,
                                              aws_session_token=AWS_SESSION_TOKEN)


@logger.inject_lambda_context
def lambda_handler(event, context):
    """
    Generates a new Petfinder access token if necessary, and returns a valid token.
    :return: Petfinder API access token.
    """
    raw_config_values = aws_variable_retriever.retrieve_parameter_value(parameter_name='configs',
                                                                        expect_json=True)
    config_values = json.loads(raw_config_values)
    logger.info("Successfully retrieved and JSON parsed config values.")

    data = {
        'grant_type': 'client_credentials',
        'client_id': config_values['pf_api_key'],
        'client_secret': config_values['pf_secret_key']
    }
    logger.info(f"Beginning Petfinder access token request.")
    response = requests.post(url=config_values['pf_token_url'],
                             data=data)
    response.raise_for_status()
    logger.info("Petfinder API request successful.")

    response_json = response.json()
    access_token = response_json['access_token']
    logger.info("Successfully retrieved Petfinder API access token from API response.")

    return access_token

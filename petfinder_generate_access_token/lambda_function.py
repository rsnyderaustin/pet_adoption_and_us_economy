from json.decoder import JSONDecodeError
import json
import os
import requests
import time

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


class MaxGenerateAccessTokenTriesError(Exception):
    pass


@logger.inject_lambda_context
def lambda_handler(event, context):
    """
    Generates a new Petfinder access token if necessary, and returns a valid token.
    :return: Petfinder API access token.
    """
    raw_config_values = aws_variable_retriever.retrieve_parameter_value(parameter_name='configs',
                                                                        expect_json=True)
    config_values = json.loads(raw_config_values)

    data = {
        'grant_type': 'client_credentials',
        'client_id': config_values['pf_api_key'],
        'client_secret': config_values['pf_secret_key']
    }
    retry_seconds = config_values['pf_access_token_retry_seconds']
    max_tries = len(retry_seconds) + 1

    for tries in range(max_tries):
        if tries >= 1:
            logger.info(f"Retry number {tries} for generating a Petfinder access token.")
            # The 0th index of retry_seconds represents the sleep time for when "tries" is 1 (the second try).
            time.sleep(retry_seconds[tries - 1])
        try:
            response = requests.post(url=config_values['pf_token_url'],
                                     data=data)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(str(e))
            return e
        try:
            response_data = response.json()
        except JSONDecodeError as e:
            logger.error(str(e))
            return e
        try:
            return response_data['access_token']
        except KeyError as e:
            logger.error(str(e))
            raise e
    logger.error(f"Max number of tries ({max_tries}) reached when generating Petfinder access token.")
    raise MaxGenerateAccessTokenTriesError

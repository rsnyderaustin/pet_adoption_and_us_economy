from json.decoder import JSONDecodeError
import json
import os
import requests
import time

from aws_lambda_powertools import Logger

from aws_cache_retrieval import AwsVariableRetriever

logger = Logger(service="petfinder_access_token_generator")


aws_session_token = os.environ['AWS_SESSION_TOKEN']
aws_region = os.environ['AWS_REGION']
cache_port = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
project_name = os.environ['FRED_PROJECT_NAME']

aws_variable_retriever = AwsVariableRetriever(cache_port=cache_port,
                                              project_name=project_name,
                                              aws_session_token=aws_session_token)


class MaxGenerateAccessTokenTriesError(Exception):
    pass


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
        'client_id': config_values['petfinder_api_key'],
        'client_secret': config_values['petfinder_secret_key']
    }
    retry_seconds = config_values['petfinder_access_token_retry_seconds']
    max_tries = len(retry_seconds) + 1

    for tries in range(max_tries):
        if tries >= 1:
            logger.info(f"Retry number {tries} for generating a Petfinder access token.")
            # The 0th index of retry_seconds represents the sleep time for when "tries" is 1 (the second try).
            time.sleep(retry_seconds[tries - 1])
        try:
            response = requests.post(url=config_values['petfinder_token_url'],
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

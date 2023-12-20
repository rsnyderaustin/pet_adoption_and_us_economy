import json.decoder

import requests
import logging
import time
from urllib.parse import urljoin

from api_pull.settings import TomlConfigLoader as ConfigLoader, TomlLogsLoader as LogsLoader
from .petfinder_api_request import PetfinderApiRequest


class MaxPetfinderDataRequestTriesError(Exception):
    pass


class MaxPetfinderTokenGenerationTriesError(Exception):
    pass


class PetfinderApiConnectionManager:

    def __init__(self, api_url, token_url):
        """

        :param api_url: Petfinder API URL
        :param token_url: Petfinder token generator URL
        """
        self.api_url = api_url
        self.token_url = token_url

    def generate_access_token(self, api_key, secret_key):
        """
        Generates a new Petfinder access token if necessary, and returns a valid token.
        :return: Petfinder API access token.
        """

        max_retries = ConfigLoader.get_config(section='petfinder_api',
                                              name='api_connection_retries')
        retry_delay = ConfigLoader.get_config(section='petfinder_api',
                                              name='api_connection_retry_delay')

        data = {
            'grant_type': 'client_credentials',
            'client_id': api_key,
            'client_secret': secret_key
        }

        log = LogsLoader.get_log(section='petfinder_api_manager',
                                 log_name='generating_new_token')
        logging.info(log)

        for tries in range(max_retries):
            if tries >= 1:
                log = LogsLoader.get_log(section='petfinder_api_manager',
                                         log_name='retrying_token_request',
                                         parameters={'retries': tries})
                logging.info(log)
                time.sleep(retry_delay)

            try:
                response = requests.post(url=self.token_url, data=data)
                response.raise_for_status()
                log = LogsLoader.get_log(section='petfinder_api_manager',
                                         log_name='successful_token_generation')
                logging.info(log)
            except requests.exceptions.RequestException as e:
                logging.error(e)
                continue

            try:
                response_data = response.json()
            except json.decoder.JSONDecodeError as json_error:
                error_msg = str(json_error)
                logging.error(error_msg)
                raise json_error
            try:
                return response_data['access_token']
            except KeyError as e:
                error_msg = str(e)
                logging.error(error_msg)
                raise e

        # End of for loop - reached if no access token is successfully generated
        log = LogsLoader.get_log(section='petfinder_api_manager',
                                 log_name='failed_to_generate_token',
                                 parameters={'num_retries': max_retries})
        logging.error(log)
        raise MaxPetfinderTokenGenerationTriesError(log)

    def make_request(self, access_token, petfinder_api_request: PetfinderApiRequest):
        """
        Sends a request to Petfinder API. If successful, returns the JSON data from the response.
        :return: If successful, returns JSON request data. If not successful, raises an error.
        """

        max_retries = ConfigLoader.get_config(section='petfinder_api',
                                              name='api_connection_retries')
        retry_delay = ConfigLoader.get_config(section='petfinder_api',
                                              name='api_connection_retry_delay')

        access_token_header = {
            'Authorization': f'Bearer {access_token}'
        }

        category = petfinder_api_request.category
        urljoin(self.api_url, category)

        parameters = petfinder_api_request.parameters

        for tries in range(max_retries):
            # Log that the system is retrying a connection
            if tries >= 1:
                log = LogsLoader.get_log(section='petfinder_api_manager',
                                         log_name='retrying_request',
                                         parameters={'retries': tries})
                logging.info(log)
                time.sleep(retry_delay)

            try:
                response = requests.get(headers=access_token_header,
                                        url=self.api_url,
                                        params=parameters)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                log = LogsLoader.get_log(section='petfinder_api_manager',
                                         log_name='failed_request',
                                         parameters={
                                             'parameters': parameters,
                                             'details': e
                                         })
                logging.error(log)
                continue

            log = LogsLoader.get_log(section='petfinder_api_manager',
                                     log_name='successful_request',
                                     parameters={
                                         'path': category,
                                         'parameters': parameters
                                     })
            logging.info(log)

            json_data = response.json()
            return json_data

        # End of for loop - this is reached if no data is successfully received
        log = LogsLoader.get_log(section='petfinder_api_manager',
                                 log_name='failed_to_make_request',
                                 parameters={'num_retries': max_retries})
        logging.error(log)
        raise MaxPetfinderDataRequestTriesError

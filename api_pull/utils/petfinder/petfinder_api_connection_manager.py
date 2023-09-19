import json.decoder

import requests
import logging
import time
from urllib.parse import urljoin

from settings import ConfigLoader, LogLoader
from utils.petfinder import petfinder_access_token


class MaxPetfinderApiConnectionTriesError(Exception):
    pass


class MaxPetfinderTokenGenerationTriesError(Exception):
    pass


class PetfinderApiConnectionManager:

    def __init__(self, api_url, token_url, api_key, secret_key):
        """

        :param api_url: Petfinder API URL
        :param token_url: Petfinder token generator URL
        :param api_key: Petfinder API key - specific to each Petfinder account.
        :param secret_key: Petfinder secret API key - specific to each Petfinder account.
        """
        self.api_url = api_url
        self.token_url = token_url
        self.api_key = api_key
        self.secret_key = secret_key

        self._access_token = None

    def _handle_access_token(self, new_token, time_of_generation, expiration) -> None:
        """

        :param new_token: Newly generated Petfinder API access token.
        :param time_of_generation: Time (seconds) of the access token generation.
        :param expiration: Time (seconds) until access token expiration.
        """
        if self._access_token:
            self._access_token.update_access_token(new_token=new_token,
                                                   time_of_generation=time_of_generation,
                                                   expiration=expiration)
        else:
            self._access_token = petfinder_access_token.PetfinderAccessToken(
                access_token=new_token,
                time_of_generation=time_of_generation,
                expiration=expiration)

    def valid_access_token_exists(self):
        return self._access_token and self._access_token.token_is_valid()

    def generate_access_token(self):
        """

        :return: Petfinder API access token.
        """

        if self.valid_access_token_exists():
            return self._access_token.get_access_token()

        max_retries = ConfigLoader.get_config(section='petfinder_api',
                                              config_name='api_connection_retries')
        retry_delay = ConfigLoader.get_config(section='petfinder_api',
                                              config_name='api_connection_retry_delay')

        data = {
            'grant_type': 'client_credentials',
            'client_id': self.api_key,
            'client_secret': self.secret_key
        }

        log = LogLoader.get_log(section='petfinder_api_manager',
                                log_name='generating_new_token')
        logging.info(log)

        for tries in range(max_retries):
            if tries >= 1:
                log = LogLoader.get_log(section='petfinder_api_manager',
                                        log_name='retrying_token_request',
                                        parameters={'retries': tries})
                logging.info(log)
                time.sleep(retry_delay)

            try:
                response = requests.post(url=self.token_url, data=data)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(e)
                continue

            log = LogLoader.get_log(section='petfinder_api_manager',
                                    log_name='successful_token_generation')
            logging.info(log)
            generation_time = time.time()
            try:
                response_data = response.json()
                self._handle_access_token(new_token=response_data["access_token"],
                                          time_of_generation=generation_time,
                                          expiration=response_data["expires_in"])
                return self._access_token.get_access_token()
            except json.decoder.JSONDecodeError as json_error:
                logging.error(json_error)
                raise
            except KeyError:
                valid_keys = ['access_token', 'expires_in']
                keys_not_in_response = [key for key in valid_keys if key not in response_data]
                log = LogLoader.get_log(section='petfinder_api_manager',
                                        log_name='access_token_invalid_key',
                                        parameters={'keys_not_in_response': keys_not_in_response,
                                                    'json_data': response_data})
                logging.error(log)

        log = LogLoader.get_log(section='petfinder_api_manager',
                                log_name='failed_to_generate_token',
                                parameters={'num_retries': max_retries})
        logging.error(log)
        raise MaxPetfinderTokenGenerationTriesError(log)

    @staticmethod
    def _valid_category(category):
        category = category.lower()
        return category in ['animals', 'organizations']

    def _generate_api_url(self, category: str):
        """

        :param category:  Which Petfinder API endpoint of 'animals' or 'organizations' to use in the URL.
        :return: Formatted Petfinder API URL to be used in an API request.
        """

        if not self._valid_category(category):
            log = LogLoader.get_log(section='petfinder_api_manager',
                                    log_name='invalid_category',
                                    parameters={
                                            'category': category
                                        })
            logging.error(log)
            raise ValueError(log)

        formatted_api_url = urljoin(self.api_url, category)

        return formatted_api_url

    @staticmethod
    def _generate_access_token_header(access_token: str):
        """

        :param access_token: Previously generated Petfinder API access token.
        :return: Access token header formatted for Petfinder API request.
        """
        return {'Authorization': f'Bearer {access_token}'}

    def make_request(self, category: str, parameters: dict):
        """

        :param category: Which Petfinder API endpoint of 'animals' or 'organizations' to be requested.
        :param parameters: The parameters specific for the request, formatted in a dict.
        :return: If successful, returns JSON request data. If not successful, raises an error.
        """

        max_retries = ConfigLoader.get_config(section='petfinder_api',
                                              config_name='api_connection_retries')
        retry_delay = ConfigLoader.get_config(section='petfinder_api',
                                              config_name='api_connection_retry_delay')

        category = category.lower()
        if not self._valid_category(category):
            log = LogLoader.get_log(section='petfinder_api_manager',
                                    log_name='invalid_category',
                                    parameters={
                                            'category': category
                                        })
            logging.error(log)
            raise ValueError(log)

        if self.valid_access_token_exists():
            access_token = self._access_token.get_access_token()
        else:
            access_token = self.generate_access_token()

        access_token_header = {
            'Authorization': f'Bearer {self._generate_access_token_header(access_token)}'
        }

        url = self._generate_api_url(category=category)

        for tries in range(max_retries):
            if tries >= 1:
                log = LogLoader.get_log(section='petfinder_api_manager',
                                        log_name='retrying_request',
                                        parameters={'retries': tries})
                logging.info(log)
                time.sleep(retry_delay)
            try:
                response = requests.get(headers=access_token_header,
                                        url=url,
                                        params=parameters)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                log = LogLoader.get_log(section='petfinder_api_manager',
                                        log_name='failed_request',
                                        parameters={
                                            'parameters': parameters,
                                            'details': e
                                        })
                logging.error(log)
                continue

            try:
                log = LogLoader.get_log(section='petfinder_api_manager',
                                        log_name='successful_request',
                                        parameters={
                                            'category': category,
                                            'parameters': parameters
                                        })
                logging.info(log)
                return response
            except json.decoder.JSONDecodeError as json_error:
                logging.error(json_error)

        log = LogLoader.get_log(section='petfinder_api_manager',
                                log_name='failed_to_make_request',
                                parameters={'num_retries': max_retries})
        logging.error(log)

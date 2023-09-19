import json.decoder

import requests
import logging
import time
from urllib.parse import urljoin

from settings import ConfigLoader, LogLoader
from utils.petfinder import petfinder_access_token


class MaxPetfinderApiConnectionTriesError(Exception):
    pass


class ApiException(Exception):
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

        max_retries = ConfigLoader.get_config(section='petfinder_api',
                                              config_name='api_connection_retries')
        retry_delay = ConfigLoader.get_config(section='petfinder_api',
                                              config_name='api_connection_retry_delay')
        for tries in range(max_retries):
            if tries >= 1:
                log = LogLoader.get_message(section='petfinder_api_manager',
                                            log_name='retrying_token_request',
                                            parameters={'retries': tries})
                logging.info(log)

            # If this isn't first try, then we know a valid access token does not exist
            if tries == 0 and self.valid_access_token_exists():
                return self._access_token.get_access_token()

            log = LogLoader.get_message(section='petfinder_api_manager',
                                        log_name='generating_new_token')
            logging.info(log)

            data = {
                'grant_type': 'client_credentials',
                'client_id': self.api_key,
                'client_secret': self.secret_key
            }

            try:
                response = requests.post(url=self.token_url, data=data)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logging.error(e)

            log = LogLoader.get_message(section='petfinder_api_manager',
                                        log_name='successful_token_generation')
            logging.info(log)
            generation_time = time.time()
            try:
                response_data = response.json()
            except json.decoder.JSONDecodeError as json_error:
                logging.error(json_error)

            self._handle_access_token(new_token=response_data.get("access_token"),
                                      time_of_generation=generation_time,
                                      expiration=response_data.get("expires_in"))
            return self._access_token.get_access_token()

    @staticmethod
    def _valid_category(category):
        return category in ['animals', 'organizations']

    def _generate_api_url(self, category: str):
        """

        :param category:  Which Petfinder API endpoint of 'animals' or 'organizations' to use in the URL.
        :return: Formatted Petfinder API URL to be used in an API request.
        """
        if not isinstance(category, str):
            log = LogLoader.get_message(section='general_logs',
                                        log_name='wrong_variable_type',
                                        parameters={
                                            'expected_type': 'string',
                                            'variable_name': 'api url category',
                                            'variable_type': type(category)
                                        })
            logging.error(log)
            raise TypeError(log)

        category = category.lower()

        if not self._valid_category(category):
            log = LogLoader.get_message(section='petfinder_api_manager',
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
        category = category.lower()
        if not self._valid_category(category):
            log = LogLoader.get_message(section='petfinder_api_manager',
                                        log_name='invalid_category',
                                        parameters={
                                            'category': category
                                        })
            logging.error(log)
            raise AttributeError(log)

        if self.valid_access_token_exists():
            access_token = self._access_token.get_access_token()
        else:
            access_token = self.generate_access_token()

        access_token_header = {
            'Authorization': f'Bearer {self._generate_access_token_header(access_token)}'
        }

        url = self._generate_api_url(category=category)

        response = requests.get(headers=access_token_header, url=url, params=parameters)
        if response.status_code == 200:
            log = LogLoader.get_message(section='petfinder_api_manager',
                                        log_name='successful_request',
                                        parameters={
                                            'category': category,
                                            'parameters': parameters
                                        })
            logging.info(log)
        else:
            log = LogLoader.get_message(section='petfinder_api_manager',
                                        log_name='failed_request',
                                        parameters={
                                            'parameters': parameters,
                                            'status_code': response.status_code
                                        })
            logging.error(log)
        return response

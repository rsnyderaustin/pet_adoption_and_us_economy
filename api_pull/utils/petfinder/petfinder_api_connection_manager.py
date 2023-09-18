import requests
import logging
import time

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
        if self._access_token and self._access_token.token_is_valid():
            return True
        return False

    def generate_access_token(self):
        """

        :return: Petfinder API access token.
        """
        if self.valid_access_token_exists():
            return self._access_token.get_access_token()

        log = LogLoader.get_message(section='petfinder_api_manager',
                                    log_name='generating_new_token')
        logging.info(log)

        data = {
            'grant_type': 'client_credentials',
            'client_id': self.api_key,
            'client_secret': self.secret_key
        }

        response = requests.post(url=self.token_url, data=data)
        if response.status_code == 200:
            log = LogLoader.get_message(section='petfinder_api_manager',
                                        log_name='successful_token_generation')
            logging.info(log)
            generation_time = time.time()
            response_data = response.json()
            self._handle_access_token(new_token=response_data.get("access_token"),
                                      time_of_generation=generation_time,
                                      expiration=response_data.get("expires_in"))
            return self._access_token.get_access_token()
        else:
            error_data = response.json()
            error_details = error_data.get("detail")

            log = LogLoader.get_message(section='petfinder_api_manager',
                                        log_name='failed_request',
                                        parameters={
                                            'parameters': data,
                                            'status_code': response.status_code,
                                            'details': error_details
                                        })

            logging.error(log)
            raise ApiException(log)

    @staticmethod
    def _valid_category(category):
        return category in ['animals', 'organizations']

    def _generate_api_url(self, category):
        """

        :param category:  Which Petfinder API endpoint of 'animals' or 'organizations' to use in the URL.
        :return: Formatted Petfinder API URL to be used in an API request.
        """
        category = category.lower()
        if not self._valid_category(category):
            log = LogLoader.get_message(section='petfinder_api_manager',
                                        log_name='invalid_category',
                                        parameters={
                                            'category': category
                                        })
            logging.error(log)
            raise ValueError(log)
        return self.api_url + category + '/'

    @staticmethod
    def _generate_access_token_header(access_token):
        """

        :param access_token: Previously generated Petfinder API access token.
        :return: Access token header formatted for Petfinder API request.
        """
        if access_token is None:
            log = LogLoader.get_message(section='pefinder_api',
                                        log_name='invalid_none_variable',
                                        parameters={
                                            'variable': 'access_token'
                                        })
            logging.error(log)
            raise ValueError(log)
        try:
            return {'Authorization': f'Bearer {access_token}'}
        except TypeError:
            passed_type = type(access_token)
            log = LogLoader.get_message(section='petfinder_api_manager',
                                        log_name='invalid_variable_type',
                                        parameters={
                                            'passed_type': passed_type
                                        })
            logging.error(log)
            raise TypeError(log)

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

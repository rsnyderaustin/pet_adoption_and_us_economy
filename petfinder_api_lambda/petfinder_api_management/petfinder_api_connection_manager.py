import json.decoder

from aws_lambda_powertools import Logger
import requests
import logging
import time
from urllib.parse import urljoin

from .petfinder_api_request import PetfinderApiRequest

class MaxGenerateAccessTokenTriesError(Exception):
    pass

class PetfinderApiConnectionManager:

    def __init__(self, api_url, token_url):
        """

        :param api_url: Petfinder API URL
        :param token_url: Petfinder token generator URL
        """
        self.api_url = api_url
        self.token_url = token_url
        self.logger = Logger(service="petfinder_api_connection_manager")

    def generate_access_token(self, api_key, secret_key, max_tries):
        """
        Generates a new Petfinder access token if necessary, and returns a valid token.
        :return: Petfinder API access token.
        """

        data = {
            'grant_type': 'client_credentials',
            'client_id': api_key,
            'client_secret': secret_key
        }

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
            except json.decoder.JSONDecodeError as e:
                self.logger.error(str(e))
                return e
            try:
                return response_data['access_token']
            except KeyError as e:
                self.logger.error(str(e))
                raise e
        self.logger.error(f"Max number of tries ({max_tries}) reached when generating Petfinder access token.")
        raise MaxGenerateAccessTokenTriesError

    def make_request(self, access_token, petfinder_api_request: PetfinderApiRequest, max_tries):
        """
        Sends a request to Petfinder API. If successful, returns the JSON data from the response.
        :return: If successful, returns JSON request data. If not successful, raises an error.
        """

        access_token_header = {
            'Authorization': f'Bearer {access_token}'
        }

        category = petfinder_api_request.category
        urljoin(self.api_url, category)

        parameters = petfinder_api_request.parameters

        for tries in range(max_tries):
            # Log that the system is retrying a connection
            if tries >= 1:
                self.logger.info(f"Retry number {tries} for making a Petfinder API request.\n"
                                 f"Request name is: {petfinder_api_request.name}")
            try:
                response = requests.get(headers=access_token_header,
                                        url=self.api_url,
                                        params=parameters)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                continue
            json_data = response.json()
            return json_data

        self.logger.error(f"Max number of tries ({max_tries}) reached when making Petfinder API request.\n"
                          f"Request name is {petfinder_api_request.name}.")

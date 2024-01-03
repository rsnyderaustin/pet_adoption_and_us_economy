import logging
from json.decoder import JSONDecodeError
import time
import requests
from urllib.parse import urljoin

from .petfinder_api_request import PetfinderApiRequest


class MaxGenerateAccessTokenTriesError(Exception):
    pass


class PetfinderApiConnectionManager:

    def __init__(self, api_url, access_token):
        """

        :param api_url: Petfinder API URL
        :param token_url: Petfinder token generator URL
        """
        self.api_url = api_url
        self.access_token = access_token
        self.logger = logging.getLogger(name="PetfinderApiConnectionManager")

    def format_url_with_category(self, category):
        return urljoin(self.api_url, category)

    def make_request(self, petfinder_api_request: PetfinderApiRequest, access_token, retry_seconds):
        """
        :return: JSON request data
        """

        access_token_header = {
            'Authorization': f'Bearer {access_token}'
        }

        api_url = self.format_url_with_category(category=petfinder_api_request.category)

        max_tries = len(retry_seconds) + 1
        for tries in range(max_tries):
            if tries >= 1:
                self.logger.info(
                    f"Beginning Petfinder API request retry number {tries} for series {petfinder_api_request.name}.")
                # The 0th index of retry_seconds represents the sleep time for when "tries" is 1 (the second try).
                time.sleep(retry_seconds[tries - 1])
            try:
                response = requests.get(headers=access_token_header,
                                        url=api_url,
                                        params=petfinder_api_request.parameters)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                continue
            try:
                json_data = response.json()
                return json_data
            except requests.exceptions.JSONDecodeError as error:
                if tries == max_tries:
                    self.logger.error(
                        f"Reached last API request try. Exception raised when decoding JSON.\nDetails: {str(error)}\n"
                        f"Exiting this API request.")
                    raise error
                else:
                    self.logger.error(f"Error when attempting to decode JSON.\nDetails: {str(error)}")

        self.logger.error(f"Max number of tries ({max_tries}) reached when making Petfinder API request.\n"
                          f"Request name is {petfinder_api_request.name}.")

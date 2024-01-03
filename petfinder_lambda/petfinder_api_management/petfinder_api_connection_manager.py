import logging
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
        :param access_token: Petfinder access token
        """
        self.api_url = api_url
        self.access_token = access_token
        self.logger = logging.getLogger(name="PetfinderApiConnectionManager")

    def process_requests_in_pages(self, headers, url, params):
        # Process the first page
        page = 1
        params['page'] = 1
        response = requests.get(headers=headers,
                                url=url,
                                params=params)
        response_json = response.json()
        total_num_pages = response_json['pagination']['total_pages']
        self.logger.info(f"Number of pages in current request: {total_num_pages}")
        page += 1

        aggregate_json_data = response_json
        for page in total_num_pages:
            params['page'] = page
            response = requests.get(headers=headers,
                                    url=url,
                                    params=params)
            response_json = response.json()
            aggregate_json_data.update(response_json)
        self.logger.info(f"{total_num_pages} successfully processed for current request.")

        return aggregate_json_data

    def format_url_with_category(self, category):
        return urljoin(self.api_url, category)

    def make_request(self, petfinder_api_request: PetfinderApiRequest, access_token, retry_seconds):
        """
        :return: JSON request data
        """

        access_token_header = {
            'Authorization': f'Bearer {access_token}'
        }

        # Have to append category to the API URL, per Petfinder API documentation
        api_url = self.format_url_with_category(category=petfinder_api_request.category)

        max_tries = len(retry_seconds) + 1
        for tries in range(max_tries):
            if tries >= 1:
                self.logger.info(
                    f"Beginning Petfinder API request retry number {tries} of {max_tries} for series "
                    f"{petfinder_api_request.name}.")
                # The 0th index of retry_seconds represents the sleep time for when "tries" is 1 (the second try).
                time.sleep(retry_seconds[tries - 1])
            try:
                json_data = self.process_requests_in_pages(headers=access_token_header,
                                                           url=api_url,
                                                           params=petfinder_api_request.parameters)
                return json_data
            except requests.exceptions.RequestException as error:
                self.logger.error(f"Exception during Petfinder API request.\nDetails: {str(error)}")
                continue
            except requests.exceptions.JSONDecodeError as error:
                self.logger.error(f"Error when attempting to decode JSON.\nDetails: {str(error)}")
                continue

        self.logger.error(f"Max number of tries ({max_tries}) reached when making Petfinder API request.\n"
                          f"Request name is {petfinder_api_request.name}. Skipping unsuccesful request.")

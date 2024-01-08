import logging
import time
import requests
from urllib.parse import urljoin

from .petfinder_api_request import PetfinderApiRequest


class MaxPfDataRequestTriesError(Exception):
    pass


class PetfinderApiConnectionManager:

    def __init__(self, api_url):
        """

        :param api_url: Petfinder API URL
        """
        self.api_url = api_url
        self.logger = logging.getLogger(name="PetfinderApiConnectionManager")

    def process_requests_by_page(self, headers, url, params):
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
        formatted_url = urljoin(self.api_url, category)
        return formatted_url

    def make_request(self, petfinder_api_request: PetfinderApiRequest, access_token):
        """
        :return: JSON request data
        """

        access_token_header = {
            'Authorization': f'Bearer {access_token}'
        }

        # Have to append category to the API URL, per Petfinder API documentation
        api_url = self.format_url_with_category(category=petfinder_api_request.category)

        json_data = self.process_requests_by_page(headers=access_token_header,
                                                  url=api_url,
                                                  params=petfinder_api_request.parameters)
        return json_data

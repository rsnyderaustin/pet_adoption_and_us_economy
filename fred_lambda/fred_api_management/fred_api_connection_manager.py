import logging
import time

import requests

from .fred_api_request import FredApiRequest


class FredApiConnectionManager:

    def __init__(self, observations_api_url: str):
        self.observations_api_url = observations_api_url
        self.logger = logging.getLogger(name='FredApiConnectionManager')

    @staticmethod
    def construct_request_params(fred_api_request: FredApiRequest, api_key: str):
        params = fred_api_request.parameters
        params['series_id'] = fred_api_request.series_id
        params['api_key'] = api_key
        return params

    def make_request(self, fred_api_request: FredApiRequest, api_key: str):
        """

        :param fred_api_request:
        :param api_key:
        :return: Request data in JSON format
        """
        request_params = self.construct_request_params(fred_api_request=fred_api_request,
                                                       api_key=api_key)

        response = requests.get(url=self.observations_api_url,
                                params=request_params)
        response.raise_for_status()
        response_data = response.json()

        return response_data

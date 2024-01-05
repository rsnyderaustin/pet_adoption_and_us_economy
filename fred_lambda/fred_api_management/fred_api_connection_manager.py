import logging
import time

import requests

from .fred_api_request import FredApiRequest


class MaxFredDataRequestTriesError(Exception):
    pass


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

    def make_request(self, fred_api_request: FredApiRequest, api_key: str, retry_seconds: list[int]):
        """

        :param fred_api_request:
        :param api_key:
        :param retry_seconds:
        :return: Request data in JSON format
        """
        request_params = self.construct_request_params(fred_api_request=fred_api_request,
                                                       api_key=api_key)

        """ 
        The 0th index of retry_seconds represents the sleep time for when "tries" is 1 (the second try).
       
            ex: 0  1 2 3 4
                   | | | |
                   v v v v
                0 [2 4 8 16]
            Index: 0 1 2 3 
        """
        max_tries = len(retry_seconds) + 1
        self.logger.info(f"Beginning FredApiConnectionManager API request for {fred_api_request.name} with max tries=\n"
                         f"{max_tries}.")
        for tries in range(max_tries):
            self.logger.info(
                f"Beginning FRED API request try number {tries} of {max_tries} for request "
                f"{fred_api_request.name}.")
            if tries >= 1:
                # The 0th index of retry_seconds represents the sleep time for when "tries" is 1 (the second try).
                time.sleep(retry_seconds[tries - 1])

            response = requests.get(url=self.observations_api_url,
                                    params=request_params)
            try:
                response_data = response.json()
            except requests.exceptions.JSONDecodeError as error:
                self.logger.error(f"FRED API error during JSON decoding for request '{fred_api_request.name} "
                                  f"on try number {tries} of {max_tries}.\nDetails:{str(error)}.\n"
                                  f"Response text:{response.text}\n")
                continue

            try:
                response.raise_for_status()
            except requests.RequestException:
                self.logger.error(f"FRED API failed request for request '{fred_api_request.name} on try "
                                  f"number {tries} of {max_tries}.\nDetails:\n{response_data['details']}")
                continue

            self.logger.info(f"FRED API successful request for request '{fred_api_request.name}'.")

        # Out of the for loop - this is reached if no data is successfully received
        self.logger.error(f"Max request attempts reached for FRED API requests for request name "
                          f"{fred_api_request.name}.")
        raise MaxFredDataRequestTriesError

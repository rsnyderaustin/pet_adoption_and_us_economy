import logging
import time

import requests

from aws_lambda_powertools import Logger

from .fred_api_request import FredApiRequest


class MaxFredDataRequestTriesError(Exception):
    pass


class FredApiConnectionManager:

    def __init__(self, api_url):
        self.api_url = api_url
        self.logger = Logger(service="FredApiConnectionManager")

    def make_request(self, fred_api_request: FredApiRequest, api_key: str,  observation_start: str, retry_seconds: list[int]):

        request_url = self.api_url

        # Params in the current AWS FRED config is just an empty dict. Placeholder for possible future params
        params = fred_api_request.parameters
        params['series_id'] = fred_api_request.series_id
        params['api_key'] = api_key
        params['observation_start'] = observation_start

        max_tries = len(retry_seconds) - 1
        for tries in range(max_tries):
            if tries >= 1:
                self.logger.info(f"Beginning FRED API request retry number {tries} for series {fred_api_request.series_id}.")
                time.sleep(retry_seconds[tries])

            try:
                response = requests.get(url=request_url,
                                        params=params)
                response.raise_for_status()
            except requests.RequestException as error:
                self.logger.error(f"FRED API failed request for series ID '{fred_api_request.series_id}.\nDetails:{str(error}")
                if tries == max_tries:
                    self.logger.error(f"Reached last API request try. Exception raised when decoding JSON.\nExiting this "
                                      f"API request.")
                    raise error
                continue
            self.logger.info(f"FRED API successful request for series ID '{fred_api_request.series_id}'.")

            try:
                json_data = response.json()
                return json_data
            except requests.exceptions.JSONDecodeError as error:
                if tries == max_tries:
                    self.logger.error(
                        f"Reached last API request try. Exception raised when decoding JSON.\nDetails: {str(error)}\n"
                        f"Exiting this "
                        f"API request.")
                    raise error
                else:
                    self.logger.error(f"Error when attempting to decode JSON.\nDetails: {str(error)}")

        # Out of the for loop - this is reached if no data is successfully received
        self.logger.error("Max request attempts reached.")
        raise MaxFredDataRequestTriesError

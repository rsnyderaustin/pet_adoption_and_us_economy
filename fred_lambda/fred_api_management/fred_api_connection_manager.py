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

    def make_request(self, api_key: str, fred_api_request: FredApiRequest, observation_start, retry_seconds: list):

        request_url = self.api_url

        # Params in the current AWS FRED config is just an empty dict. Placeholder for possible future params
        params = fred_api_request.parameters
        params['series_id'] = fred_api_request.series_id
        params['api_key'] = api_key
        params['observation_start'] = observation_start

        tries = 0
        max_tries = len(retry_seconds)
        while tries < max_tries:
            if tries >= 1:
                self.logger.info(f"FRED API request retry number {tries}.")
            try:
                response = requests.get(url=request_url,
                                        params=params)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                self.logger.error(f"FRED API failed request for series ID '{fred_api_request.series_id}.\nDetails:{e}")
                continue
            try:
                self.logger.info(f"FRED API successful request for series ID '{fred_api_request.series_id}'.")
                json_data = response.json()
                return json_data
            except requests.exceptions.JSONDecodeError as json_error:
                self.logger.info(f"Error when attempting to decode JSON.\nDetails: {json_error}")

            time.sleep(retry_seconds[tries])
            tries += 1

        # Out of the for loop - this is reached if no data is successfully received
        self.logger.error("Max request attempts reached.")
        raise MaxFredDataRequestTriesError

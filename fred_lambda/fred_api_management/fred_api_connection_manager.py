import logging
import time

import requests

from .fred_api_request import FredApiRequest


class MaxFredDataRequestTriesError(Exception):
    pass


class FredApiConnectionManager:

    def __init__(self, observations_api_url):
        self.observations_api_url = observations_api_url
        self.logger = logging.getLogger(name='FredApiConnectionManager')

    def make_request(self, fred_api_request: FredApiRequest, api_key: str,  retry_seconds: list[int]):
        """

        :param fred_api_request:
        :param api_key:
        :param retry_seconds:
        :return: Request data in JSON format
        """
        params = fred_api_request.parameters
        params['series_id'] = fred_api_request.series_id
        params['api_key'] = api_key

        # The 0th index of retry_seconds represents the sleep time for when "tries" is 1 (the second try).
        max_tries = len(retry_seconds) + 1
        for tries in range(max_tries):
            if tries >= 1:
                self.logger.info(f"Beginning FRED API request retry number {tries} for series {fred_api_request.series_id}.")
                # The 0th index of retry_seconds represents the sleep time for when "tries" is 1 (the second try).
                time.sleep(retry_seconds[tries - 1])

            try:
                response = requests.get(url=self.observations_api_url,
                                        params=params)
                response.raise_for_status()
            except requests.RequestException as error:
                self.logger.error(f"FRED API failed request for series ID '{fred_api_request.series_id}.\n"
                                  f"Details:{str(error)}")
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
                        f"Exiting this API request.")
                    raise error
                else:
                    self.logger.error(f"Error when attempting to decode JSON.\nDetails: {str(error)}")

        # Out of the for loop - this is reached if no data is successfully received
        self.logger.error("Max request attempts reached.")
        raise MaxFredDataRequestTriesError

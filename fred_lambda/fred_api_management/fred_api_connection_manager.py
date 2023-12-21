import boto3
from datetime import datetime
import logging
from typing import Union

import requests
import time
from urllib.parse import urljoin

from .fred_api_request import FredApiRequest

class MaxFredDataRequestTriesError(Exception):
    pass


class FredApiConnectionManager:

    def __init__(self, api_url):
        self.api_url = api_url

    def get_last_updated_date(self, json_data) -> Union[datetime, None]:
        """
        Get the most recent date on which data for the specified FRED API series_id was updated.

        :return: A datetime object representing the most recent update date for the specified series_id, or None if no dates
            are found.
        """

        try:
            # Retrieve only dates when the series data was revised or new data added
            dates = json_data['vintage_dates']
        except KeyError:
            log = LogsLoader.get_log(section='fred_api_manager',
                                     log_name='no_vintage_dates',
                                     parameters={
                                         'key': 'vintage_dates',
                                         'variable': 'json',
                                         'json_data': json_data
                                     })
            logging.error(log)
            raise KeyError(log)

        if len(dates) == 0:
            return None

        date_format = "%Y-%m-%d"
        try:
            datetime_dates = [datetime.strptime(date_str, date_format) for date_str in dates]
        except ValueError:
            log = LogsLoader.get_log(section='fred_api_manager',
                                     log_name='datetime_convert_error',
                                     parameters={'dates': dates})
            logging.error(log)
            raise ValueError(log)

        last_date = max(datetime_dates)
        return last_date

    def make_request(self, api_key: str, fred_api_request: FredApiRequest, observation_start, retry_delay, max_retries):
        """
        Sends an API request to the FRED API.
        :param path_segments:
        :param series_id:
        :return: Json data returned from the FRED API request.
        :raises MaxFredDataRequestTriesError: If the maximum number of API data request retries is reached without
            successfully receiving data.
        """

        request_url = self.api_url

        # Params in the current AWS FRED config is just an empty dict. Placeholder for possible future params
        params = fred_api_request.parameters
        params['series_id'] = fred_api_request.series_id
        params['api_key'] = api_key
        params['observation_start'] = observation_start

        for tries in range(max_retries):
            if tries >= 1:
                log = LogsLoader.get_log(section='fred_api_manager',
                                         log_name='retrying_request',
                                         parameters={'retries': tries})
                logging.info(log)
                time.sleep(retry_delay)
            try:
                response = requests.get(url=request_url,
                                        params=params)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                log = LogsLoader.get_log(section='fred_api_manager',
                                         log_name='failed_request',
                                         parameters={
                                             'series_id': fred_api_request.series_id,
                                             'retry': tries,
                                             'details': e
                                         })
                logging.error(log)
                continue
            try:
                json_data = response.json()
                return json_data
            except requests.exceptions.JSONDecodeError as json_error:
                logging.error(str(json_error))

            tries += 1

        # Out of the for loop - this is reached if no data is successfully received
        log = LogsLoader.get_log(section='fred_api_manager',
                                 log_name='failed_to_make_request',
                                 parameters={'num_retries': max_retries})
        logging.error(log)
        raise MaxFredDataRequestTriesError

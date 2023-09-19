from datetime import datetime
import logging
from typing import Union

import requests
import time
from urllib.parse import urljoin

from settings import logging_config, ConfigLoader, LogLoader


class MaxFredDataRequestTriesError(Exception):
    pass


class FredApiConnectionManager:

    valid_tags = ['GDP', 'RSXFS', 'UNRATE', 'CPALTT01USM657N', 'DFF']

    def __init__(self, api_url, api_key):
        self.api_url = api_url
        self.api_key = api_key

    @staticmethod
    def _is_valid_tag(tag):
        tag = tag.upper()
        return tag in FredApiConnectionManager.valid_tags

    def _generate_api_url(self, path_segments: Union[list[str], str]):
        """

        :param path_segments:  Which Petfinder API endpoint of 'animals' or 'organizations' to use in the URL.
        :return: Formatted Petfinder API URL to be used in an API request.
        """
        path = "/".join(path_segments) if isinstance(path_segments, list) else path_segments
        formatted_api_url = urljoin(self.api_url, path)
        return formatted_api_url

    def get_last_updated_date(self, tag) -> datetime:
        """
        Get the most recent date on which data for the specified FRED API tag was updated.

        :param tag: The FRED API tag for which the last updated date is requested.
        :return: A datetime object representing the most recent update date for the specified tag.
        """
        if not self._is_valid_tag(tag=tag):
            log = LogLoader.get_log(section='fred_api_manager',
                                    log_name='invalid_tag',
                                    parameters={'valid_tags': FredApiConnectionManager.valid_tags})
            logging.error(log)
            raise ValueError(log)

        json_data = self.make_request(path_segments='vintagedates',
                                      tag=tag)
        try:
            dates = json_data['vintage_dates']
        except KeyError:
            log = LogLoader.get_log(section='fred_api_manager',
                                    log_name='no_vintagedates',
                                    parameters={
                                            'key': 'vintagedates',
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
            log = LogLoader.get_log(section='fred_api_manager',
                                    log_name='datetime_convert_error',
                                    parameters={'dates': dates})
            logging.error(log)
            raise ValueError(log)

        last_date = max(datetime_dates)
        return last_date

    def make_request(self, path_segments: str, tag: str):
        """

        :param path_segments:
        :param variable:
        :param tag:
        :return: Json data from request to FRED API
        """

        max_retries = ConfigLoader.get_config(section='fred_api',
                                              config_name='api_connection_retries')
        retry_delay = ConfigLoader.get_config(section='fred_api',
                                              config_name='api_connection_retry_delay')

        data = {
            'series_id': tag,
            'api_key': self.api_key
        }

        request_url = self._generate_api_url(path_segments=path_segments)

        for tries in range(max_retries):
            if tries >= 1:
                log = LogLoader.get_log(section='fred_api_manager',
                                        log_name='retrying_request',
                                        parameters={'retries': tries})
                logging.info(log)
                time.sleep(retry_delay)
            try:
                response = requests.get(url=request_url,
                                        data=data)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                log = LogLoader.get_log(section='fred_api_manager',
                                        log_name='failed_request',
                                        parameters={
                                            'tag': tag,
                                            'retry': tries,
                                            'details': e
                                        })
                logging.error(log)
                continue
            try:
                json_data = response.json()
                return json_data
            except requests.exceptions.JSONDecodeError as json_error:
                logging.error(json_error)

        # End of for loop - this is reached if no data is successfully received
        log = LogLoader.get_log(section='fred_api_manager',
                                log_name='failed_to_make_request',
                                parameters={'num_retries': max_retries})
        logging.error(log)
        raise MaxFredDataRequestTriesError




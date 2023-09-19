import logging
import requests
from datetime import datetime

from settings import logging_config, ConfigLoader, LogLoader


class FredApiConnectionManager:

    def __init__(self, api_url, api_key):
        self.api_url = api_url
        self.api_key = api_key

    @staticmethod
    def generate_variable_tag(variable: str):
        variable_tags = {
            'Gross Domestic Product': 'GDP',
            'Retail Trade': 'RSXFS',
            'Unemployment': 'UNRATE',
            'Consumer Price Index': 'CPALTT01USM657N',
            'Interest Rate': 'DFF'
        }

        try:
            tag = variable_tags[variable]
        except KeyError:
            log = LogLoader.get_message(section='fred_api_manager',
                                        log_name='invalid_variable',
                                        parameters={
                                            'variable': variable,
                                            'valid_variables': list(variable_tags.keys())
                                        })
            logging.error(log)
            raise KeyError(log)
        return {'series_id': tag}

    def generate_request_url(self, category):
        return self.api_url + category + '/'

    def get_last_updated_date(self, variable=None, tag=None) -> datetime:
        if variable:
            tag = self.generate_variable_tag(variable=variable)

        response = self.get_fred_data(category='vintagedates',
                                      tag=tag)

        json_data = self.get_json_from_response(response)
        try:
            dates = json_data['vintage_dates']
        except KeyError:
            log = LogLoader.get_message(section='fred_api_manager',
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
            log = LogLoader.get_message(section='fred_api_manager',
                                        log_name='datetime_convert_error',
                                        parameters={'dates': dates})
            logging.error(log)
            raise ValueError(log)

        last_date = max(datetime_dates)
        return last_date

    def get_fred_data(self, category, variable=None, tag=None):
        if variable:
            tag = self.generate_variable_tag(variable=variable)

        data = {
            'series_id': tag,
            'api_key': self.api_key
        }

        request_url = self.generate_request_url(category=category)
        response = requests.get(url=request_url,
                                data=data)
        return response

    @staticmethod
    def get_json_from_response(response):
        response_json = response.json()
        return response_json



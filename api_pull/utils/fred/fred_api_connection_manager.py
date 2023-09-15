import logging
import requests

from settings import logging_config, ConfigLoader, LogLoader


class FredApiConnectionManager:

    def __init__(self, api_url, api_key):
        self.api_url = api_url
        self.api_key = api_key

    @staticmethod
    def get_variable_tag(variable: str):
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

    def get_time_series_data(self, variable=None, tag=None):
        if variable:
            tag = self.get_variable_tag(variable=variable)

        data = {
            'series_id': tag,
            'api_key': self.api_key
        }

        request_url = self.api_url + '/observations'
        response = requests.get(request_url, data=data)
        response_json = response.json()
        observations = response_json.get('observations')
        date_values = [(observation['date'], observations['value']) for observation in observations]



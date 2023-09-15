from settings import logging_config, ConfigLoader, LogLoader
import requests

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

        valid_variables = variable_tags.keys()
        if variable not in valid_variables:
            log = LogLoader.get_message(section='fred_api_manager',
                                        log_name='invalid_variable',
                                        parameters={
                                            'variable': variable,
                                            'valid_variables': valid_variables
                                        })
            raise ValueError(log)
        tag = variable_tags[variable]
        return {'series_id': tag}

    def get_time_series_data(self, variable=None, tag=None):
        if variable:
            tag = self.get_variable_tag(variable=variable)

        data = {
            'series_id': tag,
            'api_key': self.api_key
        }

        request_url = self.api_url + '/observations'
        connection_attempts = 0
        try:
            response = requests.get(request_url, data=data)
        except requests.ConnectionError:



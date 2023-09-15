from api_pull.settings import logging_config, ConfigLoader, LogLoader


class FredApiConnectionManager:

    def __init__(self, api_url, api_key):
        self.api_url = api_url
        self.api_key = api_key

    def get_variable_tag(self, variable):
        variable_tags = {
            'Gross Domestic Product': 'GDP',
            'Retail Trade': 'RSXFS',
            'Unemployment': 'UNRATE',
            'Consumer Price Index': 'CPALTT01USM657N',
            'Interest Rate': 'DFF'
        }

        valid_variables = variable_tags.keys()
        if variable not in valid_variables:
            log = LogLoader.get_message(section='')
            raise ValueError()



class FredApiRequest:

    def __init__(self, name: str, series_id: str, parameters: dict):
        self.name = name
        self.series_id = series_id
        self.parameters = parameters

    def add_parameter(self, name, value):
        self.parameters[name] = value


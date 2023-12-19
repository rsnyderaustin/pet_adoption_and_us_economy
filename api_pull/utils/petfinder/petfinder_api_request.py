

class PetfinderApiRequest:

    def __init__(self, name: str, category: str, parameters: dict):
        # 'name' is the request's identifier throughout the lifecycle
        self.name = name

        self.category = category
        self.parameters = parameters

    def add_parameter(self, parameter_name, parameter_value):
        self.parameters[parameter_name] = parameter_value

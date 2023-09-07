

class PetfinderApiRequest:

    def __init__(self, name: str, category: str, parameters: dict):
        self.name = name
        self.category = category
        self.parameters = parameters

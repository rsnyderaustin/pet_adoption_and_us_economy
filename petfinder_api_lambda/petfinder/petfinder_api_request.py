

class PetfinderApiRequest:

    def __init__(self, name: str, category: str, parameters: dict):
        # 'name' is the request's identifier throughout the lifecycle
        self.name = name

        self.category = category
        self.parameters = parameters

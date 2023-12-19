

class PetfinderAccessToken:
    _instance = None
    _update_deadline = None

    def __new__(cls, update_deadline):
        if cls._instance is None:
            cls._instance = super(PetfinderAccessToken, cls).__new__(cls)
        cls._updated_deadline = update_deadline

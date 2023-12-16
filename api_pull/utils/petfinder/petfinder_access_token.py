import time


class PetfinderAccessToken:
    _instance = None
    _access_token = None
    _time_of_generation = None
    _expiration = None

    def __new__(cls, new_token, time_of_generation, expiration):
        if not cls._instance:
            cls._instance = super(PetfinderAccessToken, cls).__new__(cls)

        cls._access_token = new_token
        cls._time_of_generation = time_of_generation
        cls._expiration = expiration

        return cls._instance

    def token_is_valid(self):
        """
        Determines whether the current token is still a valid token
        :return: True if the current token is still valid, False if it is not.
        """
        current_time = time.time()
        # Pad the expiration to avoid issues with a delay in checking validity of an access token and using it
        padded_expiration = self._expiration - 100
        is_valid = (self._time_of_generation + padded_expiration) >= current_time
        return is_valid

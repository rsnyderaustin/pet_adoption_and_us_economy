import time

from api_pull.settings import TomlLogsLoader


class PetfinderAccessToken:

    def __init__(self, access_token: str, time_of_generation: float, expiration: float):
        self.access_token = access_token
        self._time_of_generation = time_of_generation

        # Token padding
        self._expiration = expiration - 20

    def token_is_valid(self):
        """
        Determines whether the current token is still a valid token
        :return: True if the current token is still valid, False if it is not.
        """
        current_time = time.time()
        is_valid = (self._time_of_generation + self._expiration) >= current_time
        return is_valid

    def update_access_token(self, new_token: str, time_of_generation: float, expiration: float):
        self.access_token = new_token
        self._time_of_generation = time_of_generation
        self._expiration = expiration

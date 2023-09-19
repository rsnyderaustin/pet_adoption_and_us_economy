import logging
import time

import settings
from settings import LogLoader


class PetfinderAccessToken:

    def __init__(self, access_token: str, time_of_generation: float, expiration: float):
        self._access_token = access_token
        self._time_of_generation = time_of_generation

        # Token padding
        self._expiration = expiration - 20

    def token_is_valid(self):
        current_time = time.time()
        is_valid = (self._time_of_generation + self._expiration) >= current_time
        return is_valid

    def update_access_token(self, new_token: str, time_of_generation: float, expiration: float):
        self._access_token = new_token
        self._time_of_generation = time_of_generation
        self._expiration = expiration

    def get_access_token(self):
        return self._access_token

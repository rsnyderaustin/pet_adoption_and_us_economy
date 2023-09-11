import logging
import time


class PetfinderAccessToken:

    def __init__(self, access_token, time_of_generation, expiration):
        self._access_token = access_token
        self._time_of_generation = time_of_generation
        self._expiration = expiration

    def need_to_generate_new_token(self):
        current_time = time.time()
        try:
            return (self._time_of_generation + self._expiration) <= current_time
        except TypeError:
            if self._time_of_generation is None and self._expiration is None:
                logging.warning(f"PetfinderAccessToken need_to_generate_new_token method called when time of"
                                f"generation and expiration are None.")
            elif self._time_of_generation is None:
                logging.warning(f"PetfinderAccessToken need_to_generate_new_token method called when time of"
                                f"generation is None.")
            else:
                logging.warning(f"PetfinderAccessToken need_to_generate_new_token method called when expiration is None.")
            return True

    def update_access_token(self, new_token, time_of_generation, expiration):
        self._access_token = new_token
        self._time_of_generation = time_of_generation
        self._expiration = expiration

    def get_access_token(self):
        return self._access_token

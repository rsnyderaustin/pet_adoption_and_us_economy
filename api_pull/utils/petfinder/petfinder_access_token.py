import logging
import time

import settings
from settings import LogLoader


class PetfinderAccessToken:

    def __init__(self, access_token: str, time_of_generation: float, expiration: float):
        self._access_token = access_token
        self._time_of_generation = time_of_generation
        self._expiration = expiration

    def need_to_generate_new_token(self):
        if self._time_of_generation is None or self._expiration is None:
            log = LogLoader.get_message(section='petfinder_access_token',
                                        log_name='invalid_time_or_expiration',
                                        parameters={
                                            'time_of_generation': self._time_of_generation,
                                            'expiration': self._expiration
                                        })
            logging.error(log)
            raise AttributeError(log)

        current_time = time.time()
        return (self._time_of_generation + self._expiration) <= current_time

    @staticmethod
    def ensure_is_float(number):
        if not isinstance(number, float) and not isinstance(number, int):
            log = LogLoader.get_message(section='petfinder_access_token',
                                        log_name='cant_float',
                                        parameters={
                                            'number': number,
                                            'type': type(number)
                                        })
            logging.error(log)
            raise TypeError(log)

        return float(number)

    def update_access_token(self, new_token: str, time_of_generation: float, expiration: float):
        if not isinstance(new_token, str):
            log = LogLoader.get_message(section='petfinder_access_token',
                                        log_name='not_expected_type_string',
                                        parameters={
                                            'value': new_token,
                                            'type': type(new_token)
                                        })
            logging.error(log)
            raise TypeError(log)
        self._access_token = new_token
        self._time_of_generation = self.ensure_is_float(time_of_generation)
        self._expiration = self.ensure_is_float(expiration)

    def get_access_token(self):
        return self._access_token

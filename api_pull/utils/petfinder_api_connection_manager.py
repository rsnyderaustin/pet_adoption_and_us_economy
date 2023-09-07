import requests
import logging
import time
from api_pull.utils import petfinder_access_token


class PetfinderApiConnectionManager:

    def __init__(self, api_url, token_url, api_key, secret_key):
        """

        :param api_url: Petfinder API URL
        :param token_url: Petfinder token generator URL
        :param api_key: Petfinder API key - specific to each Petfinder account.
        :param secret_key: Petfinder secret API key - specific to each Petfinder account.
        """
        self.api_url = api_url
        self.token_url = token_url
        self.api_key = api_key
        self.secret_key = secret_key

        self._petfinder_access_token = None

    def _handle_access_token(self, new_token, time_of_generation, expiration) -> None:
        """

        :param new_token: Newly generated Petfinder API access token.
        :param time_of_generation: Time (seconds) of the access token generation.
        :param expiration: Time (seconds) until access token expiration.
        """
        if self._petfinder_access_token:
            self._petfinder_access_token.update_access_token(new_token=new_token,
                                                             time_of_generation=time_of_generation,
                                                             expiration=expiration)
        else:
            self._petfinder_access_token = petfinder_access_token.PetfinderAccessToken(
                access_token=new_token,
                time_of_generation=time_of_generation,
                expiration=expiration)

    def _generate_access_token(self):
        """

        :return: Petfinder API access token.
        """
        if self._petfinder_access_token and not self._petfinder_access_token.need_to_generate_new_token():
            return self._petfinder_access_token.get_access_token()

        data = {
            'grant_type': 'client_credentials',
            'client_id': self.api_key,
            'client_secret': self.secret_key
        }

        max_retries = 2
        retry_delay = 2

        for num_retries in list(range(max_retries)):
            generation_time = time.time()
            response = requests.post(url=self.token_url, data=data)
            response_data = response.json()
            if response.status_code == 200:
                self._handle_access_token(new_token=response_data.get("access_token"),
                                          time_of_generation=generation_time,
                                          expiration=response_data.get("expires_in"))
                return self._petfinder_access_token.get_access_token()
            elif num_retries < max_retries:
                logging.error(f"Petfinder API get_access_token failed on retry {num_retries} with "
                              f"status code: {response.status_code} and "
                              f"details: {response_data.get('details')}")
                time.sleep(retry_delay)
            else:
                raise Exception("Max number of Petfinder API token generator retries reached. See error log.")

    def _generate_api_url(self, category):
        """

        :param category:  Which Petfinder API endpoint of 'animals' or 'organizations' to use in the URL.
        :return: Formatted Petfinder API URL to be used in an API request.
        """
        category = category.lower()
        if category not in ['animals', 'organizations']:
            raise Exception(f"Category {category} is invalid. Must be 'animals' or 'organizations'")
        return self.api_url + category + '/'

    @staticmethod
    def _generate_access_token_header(access_token):
        """

        :param access_token: Previously generated Petfinder API access token.
        :return: Access token header formatted for Petfinder API request.
        """
        if not isinstance(access_token, str):
            passed_type = type(access_token)
            raise ValueError(f"Invalid petfinder access token type passed into "
                             f"generate_access_token_header: {passed_type}. Expected string.")
        return {'Authorization': f'Bearer {access_token}'}

    def make_request(self, category: str, parameters: dict):
        """

        :param category: Which Petfinder API endpoint of 'animals' or 'organizations' to be requested.
        :param parameters: The parameters specific for the request, formatted in a dict.
        :return: If successful, returns JSON request data. If not successful, raises an error.
        """
        category = category.lower()
        if category not in ['animals', 'organizations']:
            raise ValueError(
                f"Invalid petfinder category type passed into make_request: {category}. Expected 'animals' "
                f"or 'organizations'.")
        self._generate_access_token()
        logging.info('Petfinder access token generated successfully.')
        access_token_header = {
            'Authorization': f"Bearer {self._petfinder_access_token.get_access_token()}"
        }
        url = self.api_url + category + '/'
        response = requests.get(headers=access_token_header, url=url, params=parameters)
        if response.status_code != 200:
            raise requests.HTTPError(
                f"Petfinder request with parameters {parameters} produced status code {response.status_code}")
        else:
            logging.info(f"Petfinder API manager successfully generated response for category {category} "
                         f"parameters {parameters}.")
        return response.json

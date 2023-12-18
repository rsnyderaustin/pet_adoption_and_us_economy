import boto3
import json
import os

from api_pull import PetfinderApiConnectionManager as PfManager
from api_pull import FredApiConnectionManager as FredManager
from settings import TomlConfigLoader as ConfigLoader
from settings import TomlLogsLoader as LogsLoader

# Load in Lambda environment variables
PORT = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
AWS_SESSION_TOKEN = os.environ['AWS_SESSION_TOKEN']


def create_petfinder_manager():
    """
    Automatically pulls values from the configs file to create a Petfinder API Manager class object.

    :returns: PetfinderApiConnectionManager object
    """
    api_url = ConfigLoader.get_config(section='petfinder_api', name='api_url')
    token_url = ConfigLoader.get_config(section='petfinder_api', name='token_url')
    petfinder_api_manager = PfManager(api_url=api_url,
                                      token_url=token_url)
    return petfinder_api_manager


def create_fred_manager():
    """
    Automatically pulls values from the configs file to create a FRED API Manager class object

    :returns: FredApiConnectionManager object
    """
    api_url = ConfigLoader.get_config(section='news_api', name='api_url')
    api_key = ConfigLoader.get_config(section='news_api', name='api_key')
    news_api_manager = FredManager(api_url=api_url,
                                   api_key=api_key)
    return news_api_manager


### Define function to retrieve values from extension local HTTP server cache
def retrieve_configs(url):
    url = ('http://localhost:' + port + url)
    headers = { "X-Aws-Parameters-Secrets-Token": os.environ.get('AWS_SESSION_TOKEN') }
    response = http.request("GET", url, headers=headers)
    response = json.loads(response.data)
    return response


# Default entry point for AWS Lambda
def lambda_handler(event, context):
    parameter_url =
    config_values = retrieve_configs()

    json_data = "get_json_data_here"

    aws_bucket_name = ConfigLoader.get_config(section='aws_s3', name='bucket_name')

    s3_client = boto3.client('s3')


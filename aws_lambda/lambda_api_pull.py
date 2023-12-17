import boto3
import json

from api_pull import PetfinderApiConnectionManager as PfManager
from api_pull import FredApiConnectionManager as FredManager
from settings import TomlConfigLoader as ConfigLoader
from settings import TomlLogsLoader as LogsLoader


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


def lambda_handler(event, context):
    petfinder_api_manager = create_petfinder_manager()
    fred_api_manager = create_fred_manager()

    json_data = "get_json_data_here"

    aws_bucket_name = ConfigLoader.get_config(section='aws_s3', name='bucket_name')

    s3_client = boto3.client('s3')


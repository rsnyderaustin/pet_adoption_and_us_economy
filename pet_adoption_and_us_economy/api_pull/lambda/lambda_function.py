from api_pull.configs.toml_config_loader import TomlConfigLoader
from api_pull.configs import logging_config
from api_pull.utils.petfinder_api_connection_manager import PetfinderApiConnectionManager
from api_pull.utils.news_api_connection_manager import NewsApiConnectionManager
import boto3

import logging

# Load in config data
toml_config_loader = TomlConfigLoader()
toml_config_data = toml_config_loader.toml_config_data


def get_config_value(config_data, key_hierarchy: list):
    """

    :param config_data: Program configuration data - see config_template.toml
    :param keys: .toml keys order to access the requested config_data dict value
    :return: The value within config_data that is pointed to by the provided keys. Raises a KeyError if key(s) are not
    in the config_data dict.
    """
    try:
        for key in key_hierarchy:
            config_data = config_data[key]
        return config_data
    except KeyError:
        error_log = f"Not able to find key hierarchy {key_hierarchy} in config_data {config_data}"
        logging.error(error_log)
        raise KeyError(error_log)


def create_petfinder_manager(config_data):
    """

    :param config_data: Program configuration data - see config_template.toml
    :return: PetfinderApiManager class instance created from the provided config_data
    """
    api_url = get_config_value(config_data=config_data, key_hierarchy=['petfinder_api', 'api_url'])
    token_url = get_config_value(config_data=config_data, key_hierarchy=['petfinder_api', 'token_url'])
    api_key = get_config_value(config_data=config_data, key_hierarchy=['petfinder_api', 'api_key'])
    secret_key = get_config_value(config_data=config_data, key_hierarchy=['petfinder_api', 'secret_key'])
    petfinder_api_manager = PetfinderApiConnectionManager(api_url=api_url,
                                                          token_url=token_url,
                                                          api_key=api_key,
                                                          secret_key=secret_key)
    return petfinder_api_manager


def create_news_manager(config_data):
    """

    :param config_data: Program configuration data - see config_template.toml
    :return: NewsApiManager class instance created from the provided config_data
    """
    api_url = get_config_value(config_data=config_data, key_hierarchy=['news_api', 'api_url'])
    api_key = get_config_value(config_data=config_data, key_hierarchy=['news_api', 'api_key'])
    news_api_manager = NewsApiConnectionManager(api_url=api_url,
                                                api_key=api_key)
    return news_api_manager


def lambda_handler(event, context):
    petfinder_api_manager = create_petfinder_manager(config_data=toml_config_data)
    news_api_manager = create_news_manager(config_data=toml_config_data)

    json_data = "get_json_data_here"

    bucket_name = get_config_value(config_data=toml_config_data, key_hierarchy=['aws_s3', 'bucket_name'])
    bucket_key = get_config_value(config_data=toml_config_data, key_hierarchy=['aws_s3', 'bucket_key'])

    s3_client = boto3.client("s3")

    try:


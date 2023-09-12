import os
import tomli
import logging
from toml_logging_messages_loader import TomlLoggingMessagesLoader


# Singleton pattern for loading config file data across api_pull
class TomlConfigLoader:
    _instance = None
    _toml_config_data = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TomlConfigLoader, cls).__new__(cls)
            parent_package_path = os.path.dirname(os.path.dirname(__file__))
            config_file_path = os.path.join(parent_package_path, 'config.toml')
            if os.path.exists(config_file_path):
                with open(config_file_path, "rb") as toml_file:
                    _toml_config_data = tomli.load(toml_file)
            else:
                error_log = f"Failed to connect to toml config file at {config_file_path}. Ensure that you have created" \
                            f"a config.toml file from the provided config_template.toml."
                logging.error(error_log)
                raise FileNotFoundError(error_log)
        return cls._instance

    @staticmethod
    def get_config(section_name, config_name):
        if not TomlConfigLoader._instance:
            TomlConfigLoader()





import os
import tomli
import logging


class TomlLoggingMessagesLoader:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TomlLoggingMessagesLoader, cls).__new__(cls)
            parent_package_path = os.path.dirname(os.path.dirname(__file__))
            logging_messages_file_path = os.path.join(parent_package_path, 'logging_messages.toml')
            if os.path.exists(logging_messages_file_path):
                with open(logging_messages_file_path, "rb") as toml_file:
                    toml_logging_messages = tomli.load(toml_file)
            else:
                error_log = f"Failed to connect to toml config file at {logging_messages_file_path}. Ensure that you have created" \
                            f"a config.toml file from the provided config_template.toml."
                logging.error(error_log)
                raise FileNotFoundError(error_log)
        return cls._instance

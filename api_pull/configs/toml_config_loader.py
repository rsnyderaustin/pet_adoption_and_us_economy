import os
import tomli
import logging
from .toml_logging_messages_loader import TomlLoggingMessagesLoader as MsgLoader


class MissingConfigFileError(Exception):
    pass


class MissingConfigValueError(Exception):
    pass


# Singleton class for loading config file data across api_pull
class TomlConfigLoader:
    _instance = None
    _toml_config_data = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TomlConfigLoader, cls).__new__(cls)
            parent_package_path = os.path.dirname(os.path.dirname(__file__))
            config_file_path = os.path.join(parent_package_path, 'config.toml')
            try:
                with open(config_file_path, "rb") as toml_file:
                    _toml_config_data = tomli.load(toml_file)
            except FileNotFoundError:
                err_msg = MsgLoader.get_message(section='config_loader',
                                                message_name='no_config_file',
                                                parameters={'config_file_path': config_file_path}
                                                )
                logging.error(err_msg)
                raise MissingConfigFileError(err_msg)
        return cls._instance

    @staticmethod
    def get_config_data():
        if not TomlConfigLoader._instance:
            TomlConfigLoader()

        return TomlConfigLoader._toml_config_data

    @staticmethod
    def get_config(section_name, config_name):
        if not TomlConfigLoader._instance:
            TomlConfigLoader()

        configs = TomlConfigLoader.get_config_data()

        try:
            config = configs[section_name][config_name]
        except KeyError:
            err_msg = MsgLoader.get_message(section='config_loader',
                                            message_name='missing_config_variable',
                                            parameters={
                                                'section': section_name,
                                                'config_name': config_name
                                            }
                                            )
            logging.error(err_msg)
            raise MissingConfigValueError(err_msg)

        return config

import os
import tomli
import logging
from settings.logs.toml_logging_messages_loader import TomlLoggingMessagesLoader as MsgLoader


class MissingConfigFileError(Exception):
    pass


class MissingConfigFileValueError(Exception):
    pass


class TomlConfigLoader:
    """
        If logs_file_path is not provided, the program will access 'settings.toml' within the file folder.
    """
    _instance = None
    _toml_config_data = None

    @staticmethod
    def _generate_config_file_path():
        parent_package_path = os.path.dirname(__file__)
        config_file_path = os.path.join(parent_package_path, 'config.toml')
        return config_file_path

    @staticmethod
    def _get_config_data(file_path):
        try:
            with open(file_path, "rb") as toml_file:
                return tomli.load(toml_file)
        except FileNotFoundError:
            err_msg = MsgLoader.get_message(section='config_loader',
                                            log_name='no_config_file',
                                            parameters={'config_file_path': file_path}
                                            )
            logging.error(err_msg)
            raise MissingConfigFileError(err_msg)

    def __new__(cls, config_file_path=None):
        if cls._instance is None:
            cls._instance = super(TomlConfigLoader, cls).__new__(cls)
            if not config_file_path:
                config_file_path = cls._generate_config_file_path()

            cls._toml_config_data = cls._get_config_data(config_file_path)
        return cls._instance

    @staticmethod
    def reset():
        TomlConfigLoader._instance = None
        TomlConfigLoader._toml_config_data = None

    @staticmethod
    def get_config_data(configs_file_path=None):
        if not TomlConfigLoader._instance:
            TomlConfigLoader(configs_file_path)

        return TomlConfigLoader._toml_config_data

    @staticmethod
    def get_config(section, config_name, configs_file_path=None):
        if not TomlConfigLoader._instance:
            TomlConfigLoader(configs_file_path)

        configs = TomlConfigLoader.get_config_data()

        try:
            config = configs[section][config_name]
        except KeyError:
            err_msg = MsgLoader.get_message(section='config_loader',
                                            log_name='missing_config_variable',
                                            parameters={
                                                'section': section,
                                                'config_name': config_name
                                            }
                                            )
            logging.error(err_msg)
            raise MissingConfigFileValueError(err_msg)

        return config

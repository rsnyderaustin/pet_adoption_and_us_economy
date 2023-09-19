import os
import tomli
import logging
from settings.logs.toml_logging_messages_loader import TomlLoggingMessagesLoader as MsgLoader


class MissingConfigFileError(Exception):
    pass


class MissingConfigFileValueError(Exception):
    pass


class TomlConfigLoader:
    _instance = None
    _toml_config_data = None

    @staticmethod
    def _get_all_config_data(file_path) -> dict:
        """
        Raises MissingConfigFileError if no file is found at the given file path.
        :return: A dict of all config file data from the given file path.
        """
        try:
            with open(file_path, "rb") as toml_file:
                return tomli.load(toml_file)
        except FileNotFoundError:
            err_msg = MsgLoader.get_log(section='config_loader',
                                        log_name='no_config_file',
                                        parameters={'config_file_path': file_path}
                                        )
            logging.error(err_msg)
            raise MissingConfigFileError(err_msg)

    @staticmethod
    def _generate_config_file_path() -> str:
        parent_package_path = os.path.dirname(__file__)
        config_file_path = os.path.join(parent_package_path, 'config.toml')
        return config_file_path

    def __new__(cls, config_file_path=None):
        
        """
            If logs_file_path is not provided, the program will attempt to access 'config.toml' within the current package.
        """
        if cls._instance is None:
            cls._instance = super(TomlConfigLoader, cls).__new__(cls)
            if not config_file_path:
                config_file_path = TomlConfigLoader._generate_config_file_path()

            cls._toml_config_data = cls._get_all_config_data(config_file_path)
        return cls._instance

    @staticmethod
    def reset():
        TomlConfigLoader._instance = None
        TomlConfigLoader._toml_config_data = None

    @staticmethod
    def get_config(section, config_name, configs_file_path=None):
        if not TomlConfigLoader._instance:
            TomlConfigLoader(configs_file_path)
        
        config_data = TomlConfigLoader._toml_config_data

        try:
            config = config_data[section][config_name]
        except KeyError:
            err_msg = MsgLoader.get_log(section='config_loader',
                                        log_name='missing_config_variable',
                                        parameters={
                                                'section': section,
                                                'config_name': config_name
                                            }
                                        )
            logging.error(err_msg)
            raise MissingConfigFileValueError(err_msg)

        return config

import os
import tomli
import logging


class MissingLogsFileError(Exception):
    pass


class MissingLogMessageError(Exception):
    pass


class BadLogParametersError(Exception):
    pass


class TomlLoggingMessagesLoader:
    _instance = None
    _toml_logging_messages = None

    @staticmethod
    def _generate_logs_file_path():
        parent_package_path = os.path.dirname(__file__)
        messages_file_path = os.path.join(parent_package_path, 'logging_messages.toml')
        return messages_file_path

    @staticmethod
    def _get_logs_data(file_path):
        try:
            with open(file_path, "rb") as toml_file:
                return tomli.load(toml_file)
        except FileNotFoundError:
            err_msg = TomlLoggingMessagesLoader.get_log(section='logs_loader',
                                                        log_name='no_config_file',
                                                        parameters={'logs_file_path': file_path}
                                                        )
            logging.error(err_msg)
            raise MissingLogsFileError(err_msg)

    def __new__(cls, logs_file_path=None):
        """
            If logs_file_path is not provided, the program will access 'logs.toml' within the file folder.
            :param logs_file_path: The path to a .toml log messages file.
        """
        if cls._instance is None:
            cls._instance = super(TomlLoggingMessagesLoader, cls).__new__(cls)

            # If no logs file path is passed, attempt to auto-generate the logs file path
            if not logs_file_path:
                logs_file_path = cls._generate_logs_file_path()

            if os.path.exists(logs_file_path):
                cls._toml_logging_messages = cls._get_logs_data(logs_file_path)

        return cls._instance

    @staticmethod
    def reset():
        TomlLoggingMessagesLoader._instance = None
        TomlLoggingMessagesLoader._toml_logging_messages = None

    @staticmethod
    def get_log(section, log_name, logs_file_path=None, parameters=None, repeat=False):
        if not TomlLoggingMessagesLoader._instance:
            TomlLoggingMessagesLoader(logs_file_path)

        get_log_func = TomlLoggingMessagesLoader.get_log

        log_msgs = TomlLoggingMessagesLoader._toml_logging_messages
        try:
            msg = log_msgs[section][log_name]
        except KeyError:
            # Manually log and raise error if logging_messages_loader called itself to prevent infinite recursion
            if repeat:
                err_msg = "Error in logging messages handler, unrelated to passed error. Could not access internal " \
                          "logging error message."
                logging.error(err_msg)
                raise MissingLogMessageError(err_msg)

            err_msg = get_log_func(section='messages_logging',
                                   log_name='missing_message',
                                   parameters={
                                       'section_name': section,
                                       'message_name': log_name
                                   },
                                   repeat=True)
            logging.error(err_msg)
            return 'Missing key error'
        try:
            if parameters:
                msg = msg.format(**parameters)
        except KeyError:
            if repeat:
                err_msg = ("Error in logging messages handler, unrelated to passed error. One or more bad logging"
                           " messages parameters.")
                logging.error(err_msg)
                raise BadLogParametersError(err_msg)

            err_msg = get_log_func(section='messages_logging',
                                   log_name='bad_parameters',
                                   parameters={
                                       'parameters': parameters
                                   },
                                   repeat=True)
            logging.error(err_msg)
            raise BadLogParametersError(err_msg)

        return msg

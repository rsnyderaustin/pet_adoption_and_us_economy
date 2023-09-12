import os
import tomli
import logging


class TomlLoggingMessagesLoader:
    _instance = None
    _toml_logging_messages = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TomlLoggingMessagesLoader, cls).__new__(cls)
            parent_package_path = os.path.dirname(os.path.dirname(__file__))
            logging_messages_file_path = os.path.join(parent_package_path, 'logging_messages.toml')
            if os.path.exists(logging_messages_file_path):
                with open(logging_messages_file_path, "rb") as toml_file:
                    _toml_logging_messages = tomli.load(toml_file)
            else:
                error_log = f"Failed to connect to toml config file at {logging_messages_file_path}. Ensure that you have created" \
                            f"a config.toml file from the provided config_template.toml."
                logging.error(error_log)
                raise FileNotFoundError(error_log)
        return cls._instance

    @staticmethod
    def get_message(section_name, message_name, parameters=None, repeat=False):
        if not TomlLoggingMessagesLoader._instance:
            TomlLoggingMessagesLoader()

        get_msg_func = TomlLoggingMessagesLoader.get_message

        try:
            log_msgs = TomlLoggingMessagesLoader._toml_logging_messages
        except ValueError:
            err_msg = "Error in logging messages handler, unrelated to passed error. " \
                      "Attempted to access nonexistent toml logging messages variable."
            logging.error(err_msg)
            return err_msg

        try:
            msg = log_msgs[section_name][message_name]
            if parameters:
                msg.format(parameters)
        except KeyError:
            if repeat:
                err_msg = "Error in logging messages handler, unrelated to passed error. Could not access logging " \
                          "error message."
                logging.error(err_msg)
                return err_msg
            err_msg = get_msg_func(section_name='messages_logging',
                                   message_name='missing_message',
                                   repeat=True)
            logging.error(err_msg)
            return err_msg

        return msg



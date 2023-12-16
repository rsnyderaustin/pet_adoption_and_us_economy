import inspect
import os
import tomli
import logging


class MissingLogsFileError(Exception):
    pass


class MissingLogMessageError(Exception):
    pass


class BadLogParametersError(Exception):
    pass


class GetLogInfiniteRecursionError(Exception):
    pass


class TomlLogsLoader:
    _instance = None
    _toml_logging_messages = None

    # Only becomes True if TomlLogsLoader has been instantiated, but the provided file path wasn't found
    # and/or the internal default file path wasn't found. Prevents class from crashing the program if (technically
    # unnecessary) log messages aren't found.
    _no_toml_logs_available = False

    default_logs_path = '/api_pull/runtime_logs.log'

    @staticmethod
    def _generate_logs_file_path():
        """
        :return: A log file path generated from the current package working directory.
        """
        parent_package_path = os.path.dirname(__file__)
        messages_file_path = os.path.join(parent_package_path, 'logging_messages.toml')
        return messages_file_path

    @staticmethod
    def _get_logs_data(file_path):
        """
        Retrieves TOML log data.

        :param file_path: The file path to read toml log messages from.
        :return: A dict of TOML messages from function tomli.load.
        :raises FileNotFoundError: if the provided file path does not exist.
        """
        with open(file_path, "rb") as toml_file:
            return tomli.load(toml_file)

    @staticmethod
    def _set_up_log_format(default_logs_path):
        """
        Configures the logging format.
        """
        log_format = '%(levelname)s - %(asctime)s - %(filename)s - Function:%(funcName)s -\n    %(message)s'

        logging.basicConfig(
            # Level sets the minimum log severity to print to the file
            level=logging.DEBUG,
            format=log_format,
            filename=default_logs_path,
            filemode='a'
        )

    def __new__(cls, logs_file_path=None):
        """
        Create a new or retrieve an existing instance of TomlLoggingLoader.

        The class creator is called automatically in get_log if no instance has been created, so there is no real need to call this explicitly unless you
        want to connect a different log_file_path than the default.

        :param logs_file_path: The path to a .toml logs file. If not provided, the program will attempt
            to access the logs file at a default location.
        :return: A TomlLoggingLoader instance.
        """
        if not cls._instance:
            cls._instance = super(TomlLogsLoader, cls).__new__(cls)

            cls._set_up_log_format(default_logs_path=cls.default_logs_path)

            # If no logs file path is passed, attempt to auto-generate the logs file path
            if not logs_file_path:
                logs_file_path = cls._generate_logs_file_path()

            # At this point, either the logs file path has been provided explicitly or automatically generated.
            if not os.path.exists(logs_file_path):
                cls._no_toml_logs_available = True
            else:
                cls._no_toml_logs_available = False
                cls._toml_logging_messages = cls._get_logs_data(logs_file_path)

        return cls._instance

    @classmethod
    def reset(cls):
        """
        Resets the current class instance.
        """
        cls._instance = None
        cls._toml_logging_messages = None

    @staticmethod
    def in_recursive_call() -> bool:
        """
        Returns whether the calling function has been called recursively.
        """
        current_frame = inspect.currentframe()

        prev_caller_frame = inspect.getouterframes(current_frame)[2]
        current_caller_frame = inspect.getouterframes(current_frame)[1]

        prev_function_name = prev_caller_frame.function
        current_function_name = current_caller_frame.function

        return prev_function_name == current_function_name

    @classmethod
    def get_log(cls, section: str, log_name: str, parameters=None) -> str:
        """
        Get a log message.

        :param str section: Section name from the logs file of the message being requested.
        :param str log_name: Individual log name from the logs file of the message being requested.
        :param dict parameters: Parameters to be filled into the format string of the message being requested.

        :return: The requested log message.

        :raises GetLogInfiniteRecursionError: If an error is raised during get_log, and the default internal
            get_log message is also not found.
        :raises MissingLogMessageError: If a log message is requested but not found.
        :raises BadLogParametersError: If one or more parameters are provided but do not fit into the retrieved
            log message.
        """
        if not TomlLogsLoader._instance:
            cls.__new__(TomlLogsLoader)

        if cls._no_toml_logs_available:
            return "TomlLoggingLoader class variable _no_toml_logs_available set to True, which indicates that " \
                   "no Toml logs file was found."

        log_msgs = TomlLogsLoader._toml_logging_messages
        try:
            msg = log_msgs[section][log_name]
        except KeyError:
            # Manually log and raise error if logging_messages_loader called itself and failed to find the log
            # message detailing logging_messages_loader failing internally. Prevents infinite recursion.
            if cls.in_recursive_call():
                err_msg = f"Could not access internal TomlLogsLoader error, unrelated to original error message.\n" \
                          f"Original get_log call section name was '{section}' and original message name was '{log_name}'"
                logging.error(err_msg)
                raise GetLogInfiniteRecursionError(err_msg)

            err_msg = cls.get_log(section='messages_logging',
                                  log_name='missing_message',
                                  parameters={
                                      'section_name': section,
                                      'message_name': log_name
                                  })
            logging.error(err_msg)
            raise MissingLogMessageError(err_msg)
        try:
            if parameters:
                msg = msg.format(**parameters)
        except KeyError:
            if cls.in_recursive_call():
                err_msg = "Error in logging messages handler, unrelated to passed error. Could not access internal " \
                          "logging error message."
                logging.error(err_msg)
                raise GetLogInfiniteRecursionError(err_msg)

            err_msg = cls.get_log(section='messages_logging',
                                  log_name='bad_parameters',
                                  parameters={
                                      'parameters': parameters
                                  })
            logging.error(err_msg)
            raise BadLogParametersError(err_msg)

        return msg + '\n'

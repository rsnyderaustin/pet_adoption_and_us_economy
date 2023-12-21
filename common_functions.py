

def retrieve_aws_parameter(env_variable_name, parameter_is_secret=False):
    """
    :param env_variable_name: The name of the environment variable storing the parameter name. A bit confusing,
        but it is formatted this way: 'environment variable -> AWS Parameter Store name -> [AWS Parameter Store value]'.
    :param parameter_is_secret: Whether the value stored in AWS Parameter Store is a SecureString.
    :return:
    """
    parameter_name = os.environ[env_variable_name].lower()
    request_url = f'{base_param_url}%2F{project_name}%2f{parameter_name}/'

    if parameter_is_secret:
        headers = {"X-Aws-Parameters-Secrets-Token": os.environ.get(aws_session_token)}
        request_url += '&withDecryption=True'
        response = http.request("GET", request_url, headers=headers)
    else:
        response = http.request("GET", request_url)
    try:
        response.raise_for_status()
    except HTTPError as e:
        error_msg = LogsLoader.get_log(section='aws_lambda',
                                       log_name='parameter_request_http_error',
                                       parameters={'message': e})
        logging.error(error_msg)
        raise e
    except Exception as e:
        error_msg = LogsLoader.get_log(section='aws_lambda',
                                       log_name='parameter_request_other_error',
                                       parameters={'message': e})
        logging.error(error_msg)
        raise e

    try:
        value = response['Parameters'][0]['Value']
    except KeyError:
        error_msg = LogsLoader.get_log(section='aws_lambda',
                                       log_name='parameter_value_not_found')
        logging.error(error_msg)
        raise KeyError

    return value
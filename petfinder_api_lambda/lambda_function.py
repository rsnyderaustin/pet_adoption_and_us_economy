import os
import urllib3
from urllib3.err
from requests.
from aws_lambda_powertools import Logger
from .petfinder_api_management.petfinder_api_connection_manager import PetfinderApiConnectionManager as PfManager

logger = Logger(service="petfinder_api_pull")

aws_session_token = os.environ['AWS_SESSION_TOKEN']
aws_region = os.environ['AWS_REGION']
cache_port = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
project_name = os.environ['PARAMS_PROJECT_NAME']
http = urllib3.PoolManager()
base_param_url = f'http://localhost:{cache_port}/systemsmanager/parameters/get?name='
petfinder_lifecycle_name = os.environ['PETFINDER_LIFECYCLE_NAME']
fred_lifecycle_name = os.environ['FRED_LIFECYCLE_NAME']

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
        logger.error(str(e))
        raise e
    except Exception as e:
        logger.error(str(e))

    try:
        value = response['Parameters'][0]['Value']
    except KeyError as e:
        logger.error(str(e))
        raise KeyError

    return value

pf_api_url = retrieve_aws_parameter(env_variable_name='PETFINDER_API_URL')
pf_access_token_url = retrieve_aws_parameter(env_variable_name='PETFINDER_ACCESS_TOKEN_URL')
pf_manager = PfManager(api_url=pf_api_url,
                       token_url=pf_access_token_url)

pf_api_key = retrieve_aws_parameter(env_variable_name='PETFINDER_API_KEY')
pf_secret_key = retrieve_aws_parameter(env_variable_name='PETFINDER_SECRET_KEY')

dynamodb_table_name = retrieve_aws_parameter(env_variable_name='DYNAMODB_TABLE_NAME')
dynamodb_partition_key = retrieve_aws_parameter(env_variable_name='DYNAMODB_PARTITION_KEY')
dynamodb_sort_key = retrieve_aws_parameter(env_variable_name='DYNAMODB_SORT_KEY')
dynamodb_date_format = retrieve_aws_parameter(env_variable_name='DYNAMODB_DATE_STRING_FORMAT')

@logger.inject
def lambda_handler(event, context):
    pass
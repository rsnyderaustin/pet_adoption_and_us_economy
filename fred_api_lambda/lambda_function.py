import boto3
import os
import urllib3

aws_session_token = os.environ['AWS_SESSION_TOKEN']
aws_region = os.environ['AWS_REGION']
cache_port = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
project_name = os.environ['PARAMS_PROJECT_NAME']
http = urllib3.PoolManager()
base_param_url = f'http://localhost:{cache_port}/systemsmanager/parameters/get?name='
fred_lifecycle_name = os.environ['FRED_LIFECYCLE_NAME']

fred_api_url = retrieve_aws_parameter(env_variable_name='FRED_API_URL')
fred_manager = FredManager(api_url=fred_api_url)

fred_api_key = retrieve_aws_parameter(env_variable_name='FRED_API_KEY')

fred_api_max_retries = retrieve_aws_parameter(env_variable_name='FRED_API_MAX_RETRIES')
fred_api_request_retry_delay = retrieve_aws_parameter(env_variable_name='FRED_API_REQUEST_RETRY_DELAY')
fred_output_date_format = retrieve_aws_parameter(env_variable_name='FRED_API_OUTPUT_DATE_FORMAT')

dynamodb_table_name = retrieve_aws_parameter(env_variable_name='DYNAMODB_TABLE_NAME')
dynamodb_partition_key = retrieve_aws_parameter(env_variable_name='DYNAMODB_PARTITION_KEY')
dynamodb_sort_key = retrieve_aws_parameter(env_variable_name='DYNAMODB_SORT_KEY')
dynamodb_date_format = retrieve_aws_parameter(env_variable_name='DYNAMODB_DATE_STRING_FORMAT')

import boto3
import json
import os
import urllib3
import requests
from aws_lambda_powertools import Logger

from common_functions import aws_retrieval

from .petfinder_api_management.petfinder_api_connection_manager import PetfinderApiConnectionManager as PfManager
from .petfinder_api_management.petfinder_api_request import PetfinderApiRequest as PfRequest

logger = Logger(service="petfinder_api_pull")

aws_session_token = os.environ['AWS_SESSION_TOKEN']
aws_region = os.environ['AWS_REGION']
cache_port = os.environ['PARAMETERS_SECRETS_EXTENSION_HTTP_PORT']
project_name = os.environ['PARAMS_PROJECT_NAME']
http = urllib3.PoolManager()
base_param_url = f'http://localhost:{cache_port}/systemsmanager/parameters/get?name='
petfinder_lifecycle_name = os.environ['PETFINDER_LIFECYCLE_NAME']
fred_lifecycle_name = os.environ['FRED_LIFECYCLE_NAME']


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

request_configs_bucket_name = retrieve_aws_parameter(env_variable_name='REQUEST_CONFIGS_BUCKET_NAME')
request_configs_bucket_key = retrieve_aws_parameter(env_variable_name='REQUEST_CONFIGS_BUCKET_KEY')

api_requests = retrieve_request_configs(bucket_name=request_configs_bucket_name,
                                        bucket_key=request_configs_bucket_key)
max_tries = retrieve_aws_parameter(env_variable_name='PETFINDER_MAX_API_TRIES')

pf_requests = api_requests[petfinder_lifecycle_name]

def lambda_handler(event, context):
    pf_access_token = pf_manager.generate_access_token(api_key=pf_api_key,
                                                       secret_key=pf_secret_key)

    for request in pf_requests:
        request_json_data = pf_manager.make_request(access_token=pf_access_token,
                                                    petfinder_api_request=request,
                                                    max_tries=max_tries)
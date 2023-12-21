
from common_functions import retrieve_aws_parameter

pf_api_url = retrieve_aws_parameter(env_variable_name='PETFINDER_API_URL')
pf_access_token_url = retrieve_aws_parameter(env_variable_name='PETFINDER_ACCESS_TOKEN_URL')
pf_manager = PfManager(api_url=pf_api_url,
                       token_url=pf_access_token_url)

pf_api_key = retrieve_aws_parameter(env_variable_name='PETFINDER_API_KEY')
pf_secret_key = retrieve_aws_parameter(env_variable_name='PETFINDER_SECRET_KEY')

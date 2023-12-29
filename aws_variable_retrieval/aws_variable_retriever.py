import aws_lambda_powertools
import json
import urllib3
import urllib

logger = aws_lambda_powertools.Logger()


class AwsVariableRetriever:

    def __init__(self, cache_port, project_name, aws_session_token):
        self.project_name = project_name
        self.aws_session_token = aws_session_token

        self.pool_manager = urllib3.PoolManager()

        self.base_param_url = f'http://localhost:{cache_port}/systemsmanager/parameters/get?name='
        self.base_secret_url = f'http://localhost:{cache_port}/secretsmanager/get?secretId='

    def retrieve_parameter_value(self, parameter_name, expect_json=False):
        parameter_request_url = f'{self.base_param_url}%2f{self.project_name}%2f{parameter_name}'
        url = urllib.parse.urljoin(base=self.base_param_url, url=parameter_request_url)

        header = {"X-Aws-Parameters-Secrets-Token": self.aws_session_token}

        logger.info(f"Retrieving Petfinder AWS Parameter values from extension '{parameter_request_url}'.")
        response = self.pool_manager.request("GET", url, headers=header)
        if expect_json:
            json_data = json.loads(response.data)
            return json_data
        else:
            parameter_value = response.data.decode('utf-8')
            return parameter_value

    def retrieve_secret_value(self, secret_name, expect_json=False):
        secret_request_url = f'{self.base_secret_url}%2F{self.project_name}%2f{secret_name}'
        url = urllib.parse.urljoin(base=self.base_secret_url, url=secret_request_url)

        header = {"X-Aws-Parameters-Secrets-Token": self.aws_session_token}

        logger.info(f"Retrieving AWS Secret value from extension '{secret_request_url}'.")
        response = self.pool_manager.request("GET", url, headers=header)
        if expect_json:
            json_data = json.loads(response.data)
            return json_data
        else:
            parameter_value = response.data.decode('utf-8')
            return parameter_value

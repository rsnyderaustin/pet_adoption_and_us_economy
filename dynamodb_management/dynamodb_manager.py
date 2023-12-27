import boto3
from datetime import datetime
import json

from typing import Union


class DynamoDbManager:

    def __init__(self, table_name, region, partition_key_name, sort_key_name):
        dynamodb_client = boto3.resource('dynamodb', region_name=region)
        self.dynamodb_table = dynamodb_client.Table(table_name)
        self.partition_key_name = partition_key_name
        self.sort_key_name = sort_key_name

    def get_last_updated_day(self, partition_key_value, values_attribute_name) -> Union[datetime, None]:
        response = self.dynamodb_table.query(
            KeyConditionExpression="#pk = :pk AND #sk = :sk",
            ExpressionAttributeNames={
                '#pk': self.partition_key_name,
                '#sk': self.sort_key_name
            },
            ExpressionAttributeValues={
                ":pk": {'S': partition_key_value}
            },
            # Sort dates in descending order (most recent at the top)
            ScanIndexForward=False,
            Limit=1
        )
        if response and 'Items' in response:
            last_item = response['Items'][0]
            last_month_data = last_item[values_attribute_name]
            json_data = json.loads(last_month_data)

            dates = [datetime.strptime(date_str, '%Y-%m-%d') for date_str in json_data.keys()]
            latest_date = max(dates)

            return latest_date
        else:
            return None

    def put_fred_data(self, request_name, data, values_attribute_name):
        new_data = []
        for observation in data:
            new_item = {
                self.partition_key_name: request_name,
                self.sort_key_name: observation['date'],
                values_attribute_name: observation['value']
            }
            new_data.append(new_item)


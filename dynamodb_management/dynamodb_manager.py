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
            latest_date_obj = max(dates)
            return latest_date_obj
        else:
            # Return None if no data was found for the provided partition key
            return None

    def put_fred_data(self, observations_data, partition_key_value, values_attribute_name):
        with self.dynamodb_table.batch_writer() as batch:
            for observation in observations_data:
                new_item = {
                    self.partition_key_name: partition_key_value,
                    self.sort_key_name: observation['date'],
                    values_attribute_name: observation['value']
                }
                batch.put_item(new_item)

    def put_pf_data(self, data: dict, partition_key_value, values_attribute_name):
        with self.dynamodb_table.batch_writer() as batch:
            for year_month, days_dict in data.items():
                item_data = {
                    self.partition_key_name: partition_key_value,
                    self.sort_key_name: year_month,
                    values_attribute_name: days_dict
                }
                batch.put_item(item_data)



import boto3
from datetime import datetime, timedelta

from typing import Union


class DynamoDbHandler:

    def __init__(self, table_name, region, partition_key_name, sort_key_name, sort_key_date_format):
        dynamodb_client = boto3.resource('dynamodb', region_name=region)
        self.dynamodb_table = dynamodb_client.Table(table_name)
        self.partition_key_name = partition_key_name
        self.sort_key_name = sort_key_name
        self.date_format = sort_key_date_format

    def get_last_updated_day(self, partition_key_value) -> Union[datetime, None]:
        """

        :param dynamodb_table:
        :param partition_key_name:
        :param partition_key_value:
        :param sort_key_name:
        :param date_today:
        :param date_format:
        :return: The date object of the last updated day in the DynamoDB table, or None if no date is found within 1000 days.
        """
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
            last_updated_day = last_item[self.sort_key_name]
            last_updated_day_object = datetime.strptime(last_updated_day, self.date_format)
            return last_updated_day_object
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
        

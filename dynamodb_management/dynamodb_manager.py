import boto3
from datetime import datetime
import logging

from typing import Union


class DynamoDbManager:

    def __init__(self, table_name: str, region: str, partition_key_name: str, sort_key_name: str):
        dynamodb_client = boto3.resource('dynamodb', region_name=region)
        self.dynamodb_table = dynamodb_client.Table(table_name)
        self.partition_key_name = partition_key_name
        self.sort_key_name = sort_key_name

    def get_most_recent_day(self, partition_key_value: str, values_attribute_name: str) -> Union[datetime, None]:
        response = self.dynamodb_table.query(
            ExpressionAttributeNames={
                '#pk': self.partition_key_name,
                '#sk': self.sort_key_name,
                '#attribute_name': values_attribute_name
            },
            ExpressionAttributeValues={
                ":pk": {'S': partition_key_value}
            },
            KeyConditionExpression="#pk = :pk",
            ProjectionExpression='#sk, #attribute_name',
            # Sort dates in descending order (most recent at the top)
            ScanIndexForward=False
        )
        """
        Expected format:
        {
            'Items': [
                {
                    'sk' (<- sort_key_name): {
                        'S': '2023-12'
                    }
                    'value' (<- values_attribute_name): {
                        'M': {
                            '01': {'N': '12.3'},
                            '02': {'N': '12.1'},
                            etc...
                        }
                    }
                }
            ]
        }
        """

        # Return None if no data was found for the query - likely indicates that no value is present for the partition
        # key
        if len(response['Items']) == 0:
            return None
        try:
            most_recent_item = response['Items'][0]
            year_month = most_recent_item[self.sort_key_name]['S']
            days_dict = most_recent_item[values_attribute_name]['M']
        except KeyError as error:
            logging.error(f"Key Error details: {str(error)}\nResponse item:{response}")
            raise error

        day = max(int(key) for key, value in days_dict.items())
        full_date = f"{year_month}-{day}"
        logging.info(f"Most recent updated day for DynamoDB partition key {self.partition_key_name} found to be: "
                     f"{full_date}.")
        most_recent_day = datetime.strptime(full_date, "%Y-%m-%d")
        return most_recent_day

    def put_fred_data(self, data: dict, partition_key_value: str, values_attribute_name: str):
        with self.dynamodb_table.batch_writer() as batch:
            for year_month, days_dict in data.items():
                new_item = {
                    self.partition_key_name: partition_key_value,
                    self.sort_key_name: year_month,
                    values_attribute_name: days_dict
                }
                batch.put_item(new_item)

    def put_pf_data(self, data: dict, partition_key_value: str, values_attribute_name: str):
        with self.dynamodb_table.batch_writer() as batch:
            for year_month, days_dict in data.items():
                item_data = {
                    self.partition_key_name: partition_key_value,
                    self.sort_key_name: year_month,
                    values_attribute_name: days_dict
                }
                batch.put_item(item_data)



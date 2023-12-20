import boto3
from datetime import datetime
from typing import Union


class DynamoDbHandler:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DynamoDbHandler, cls).__new__(cls)
        return cls._instance

    def get_last_updated_day(self, dynamodb_table, partition_key_name, partition_key_value, sort_key_name, date_format: str) \
            -> Union[datetime, None]:
        """

        :param dynamodb_table:
        :param partition_key_name:
        :param partition_key_value:
        :param sort_key_name:
        :param date_today:
        :param date_format:
        :return: The date object of the last updated day in the DynamoDB table, or None if no date is found within 1000 days.
        """
        response = dynamodb_table.query(
            KeyConditionExpression="#pk = :pk AND #sk = :sk",
            ExpressionAttributeNames={
                '#pk': partition_key_name,
                '#sk': sort_key_name
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
            last_updated_day = last_item[sort_key_name]
            last_updated_day_object = datetime.strptime(last_updated_day, date_format)
            return last_updated_day_object
        else:
            return None

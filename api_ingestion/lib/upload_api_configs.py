import json
from boto3.dynamodb.types import TypeSerializer
import boto3


def _convert_dictionary_to_ddb_item(api_config: dict):
    serializer = TypeSerializer()
    return {key: serializer.serialize(val) for key, val in api_config.items()}


def upload_api_configs_to_dynamodb():
    ddb_client = boto3.resource("dynamodb")
    api_details_table = ddb_client.Table("api-details")
    with open("../api_config.json", "r") as api_config_file:
        api_config_list = json.load(api_config_file)
        for config_dict in api_config_list:
            ddb_item = _convert_dictionary_to_ddb_item(config_dict)
            api_details_table.put_item(
                Item=ddb_item, ConditionExpression="attribute_not_exists(endpoint)"
            )

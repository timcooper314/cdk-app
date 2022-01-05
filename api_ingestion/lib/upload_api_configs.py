import json
import boto3


def upload_api_configs_to_dynamodb():
    print("Uploading api configs to dynamodb...")
    ddb_client = boto3.resource("dynamodb")
    api_details_table = ddb_client.Table("api-details")
    with open("./api_ingestion/lib/api_config.json", "r") as api_config_file:
        api_config_list = json.load(api_config_file)
        for config_dict in api_config_list:
            api_details_table.put_item(
                Item=config_dict, ConditionExpression="attribute_not_exists(endpoint)"
            )


if __name__ == "__main__":
    upload_api_configs_to_dynamodb()

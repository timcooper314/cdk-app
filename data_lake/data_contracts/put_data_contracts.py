import json
import logging
from pathlib import Path
import boto3

# TODO: Need a better way to get table name...
DATA_CONTRACTS_TABLE_NAME = "DataLakeStack-datacontracts427DCE3D-I8M3UEA9WB2Y"
LOGGER = logging.getLogger("PutDataContracts")
LOGGER.setLevel(logging.DEBUG)


def main():
    dynamodb = boto3.resource('dynamodb')
    data_contracts_table = dynamodb.Table(DATA_CONTRACTS_TABLE_NAME)
    path_list = Path("./data_lake/data_contracts/").rglob("*.json")
    for path in path_list:
        with path.open() as f:
            dc_json = json.load(f)
            LOGGER.debug(f"Putting data contract with key_name {dc_json['key_name']} into dynamodb table.")
            response = data_contracts_table.put_item(Item=dc_json)
    LOGGER.debug("Success")


if __name__ == '__main__':
    main()

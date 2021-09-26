import os
import json
import logging
import urllib.parse
import urllib3
import boto3


class RawToStaging:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.staging_bucket = os.getenv("STAGING_BUCKET_NAME")
        self.data_contracts_table_name = os.getenv("DATA_CONTRACTS_TABLE_NAME")
        self.s3 = boto3.client("s3")
        self.dynamodb = boto3.resource("dynamodb")

    def data_contract_exists(self, key):
        key_name = key.split("/")[0]
        data_contracts_table = self.dynamodb.Table(self.data_contracts_table_name)
        try:
            self.logger.debug(f"Verifying {key_name=} in data contract table...")
            response = data_contracts_table.get_item(Key={"key_name": key_name})
            return True
        except Exception as e:
            self.logger.warning(
                f"No data contract found for {key_name=}, with error {e}"
            )
            return False

    def copy_file_to_staged(self, key, bucket):
        self.logger.debug(
            f"Copying file with {key=} from {bucket} to {self.staging_bucket}..."
        )
        self.s3.copy({"Bucket": bucket, "Key": key}, self.staging_bucket, key)

    def delete_file_from_raw(self, key, bucket):
        self.logger.debug(f"Deleting file with {key=} from {bucket}...")
        self.s3.delete_object(Key=key, Bucket=bucket)

    def raw_to_staging(self, sqs_event):
        """Takes sqs event as input"""
        self.logger.info("Starting lambda execution...")
        for sqs_record in sqs_event["Records"]:
            for record in json.loads(sqs_record["body"])["Records"]:
                bucket = record["s3"]["bucket"]["name"]
                key = urllib.parse.unquote_plus(
                    record["s3"]["object"]["key"], encoding="UTF-8"
                )
                if self.data_contract_exists(key):
                    # TODO: Verify data contract fields...
                    self.copy_file_to_staged(key, bucket)
                    self.delete_file_from_raw(key, bucket)
        self.logger.info("Finished lambda execution.")
        return "SUCCESS"


def lambda_handler(event, context):
    return RawToStaging().raw_to_staging(event)

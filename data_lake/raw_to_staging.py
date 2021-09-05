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
        self.s3 = boto3.client('s3')

    def copy_file_to_staged(self, key, bucket):
        self.logger.debug("Copying file with {key=}...")
        self.s3.copy({"Bucket": bucket, "Key": key},
                     self.staging_bucket,
                     key)

    def raw_to_staging(self, sqs_event):
        """Takes sqs event as input"""
        self.logger.info("Starting lambda execution...")
        for sqs_record in sqs_event["Records"]:
            for record in json.loads(sqs_record["body"])["Records"]:
                bucket = record["s3"]["bucket"]["name"]
                key = urllib.parse.unquote_plus(record["s3"]["object"]["key"], encoding="UTF-8")
                self.copy_file_to_staged(key, bucket)
        self.logger.info("Finished lambda execution.")
        return "SUCCESS"


def lambda_handler(event, context):
    return RawToStaging().raw_to_staging(event)

import os
import json
import logging
import urllib.parse
from datetime import datetime
import boto3

LOGGER = logging.getLogger("SpotifyDataPreprocessor")
LOGGER.setLevel(logging.DEBUG)


def _get_endpoint_from_key(key):
    return "/".join(key.split("/")[1:3])


def _create_s3_key(landing_key, s3_key_format):
    filename = landing_key.split("_")[-1]
    dt_str = filename.split(".")[0]
    datetime_obj = datetime.strptime(dt_str, "%Y%m%d")
    return datetime_obj.strftime(s3_key_format)


def _process_top_data(json_object: dict, spotify_type: str) -> dict:
    spotify_items = json_object["items"]
    new_json = {}
    if spotify_type == "tracks":
        # TODO: make a dataclass, and display this like {{'artist': , 'track': }, {},...}
        for i, item in enumerate(spotify_items):
            new_json.update({i + 1: {item["name"]: item["artists"][0]["name"]}})
    elif spotify_type == "artists":
        for i, item in enumerate(spotify_items):
            # TODO: store the preprocessing commands in the api configs in ddb
            new_json.update({i + 1: item["name"]})
    return new_json


class SpotifyDataPreprocessor:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.raw_bucket = os.getenv("RAW_BUCKET_NAME")
        self.s3 = boto3.client("s3")
        self.api_details_table = os.getenv("API_DETAILS_TABLE")
        self.dynamodb = boto3.resource("dynamodb")

    def _put_to_s3_raw(self, json_object, s3_key):
        self.logger.debug(f"Putting {s3_key=} into bucket {self.raw_bucket}")
        self.s3.put_object(
            Body=json.dumps(json_object), Key=s3_key, Bucket=self.raw_bucket
        )
        return

    def _get_landing_data(self, key, bucket) -> dict:
        self.logger.debug(f"Getting {key=} from {bucket=}")
        s3_obj = self.s3.get_object(Key=key, Bucket=bucket)
        return json.loads(s3_obj["Body"].read())

    def _get_api_details(self, endpoint: str) -> dict:
        api_table = self.dynamodb.Table(self.api_details_table)
        self.logger.debug(
            f"Retrieving details for {endpoint} in table {self.api_details_table}..."
        )
        return api_table.get_item(
            Key={"endpoint": endpoint},
        )["Item"]

    def start_preprocessing(self, event):
        self.logger.info("Starting lambda execution...")
        for sqs_record in event["Records"]:
            records = json.loads(sqs_record["body"])["Records"]
            for s3_landing_record in records:
                landing_s3_key = urllib.parse.unquote_plus(
                    s3_landing_record["s3"]["object"]["key"], encoding="utf-8"
                )
                landing_bucket = s3_landing_record["s3"]["bucket"]["name"]

                self.logger.debug(
                    f"Processing event for {landing_s3_key=} in {landing_bucket=}..."
                )
                endpoint = _get_endpoint_from_key(landing_s3_key)
                preprocessing_details = self._get_api_details(endpoint)
                spotify_type = endpoint.split("/")[0]  # tracks or artists
                s3_key_format = preprocessing_details["s3_key_format"]
                landing_json = self._get_landing_data(landing_s3_key, landing_bucket)
                processed_json = _process_top_data(landing_json, spotify_type)
                raw_s3_key = _create_s3_key(landing_s3_key, s3_key_format)
                self._put_to_s3_raw(processed_json, raw_s3_key)

        self.logger.info("Finished lambda execution.")
        return "SUCCESS"


def lambda_handler(event, context):
    return SpotifyDataPreprocessor().start_preprocessing(event)

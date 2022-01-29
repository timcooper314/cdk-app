import os
import re
import json
import logging
import urllib.parse
from datetime import datetime
import boto3

LOGGER = logging.getLogger("SpotifyDataPreprocessor")
LOGGER.setLevel(logging.DEBUG)


def _get_endpoint_from_key(key):
    filename = key.split("/")[-1]
    endpoint_key = re.findall("[a-zA-z]+", filename)[0]
    return endpoint_key.split("_")[-1]


def _process_top_data(json_object, processing_details):
    endpoint = processing_details["endpoint"]
    spotify_items = json_object["items"]
    new_json = {}
    if endpoint == "tracks":
        # TODO: make a dataclass, and display this like {{'artist': , 'track': }, {},...}
        for i, item in enumerate(spotify_items):
            new_json.update({i + 1: {item["name"]: item["artists"][0]["name"]}})
    elif endpoint == "artists":
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

    def _put_to_s3_raw(self, json_object, endpoint, filename):
        datetime_now = datetime.now().strftime("%Y%m%d")
        month_now = datetime.now().strftime("%m")
        s3_key = f"spotify/{endpoint}/{month_now}/{filename}{datetime_now}.json"
        self.logger.debug(f"Putting {s3_key=} into bucket {self.raw_bucket}")
        self.s3.put_object(
            Body=json.dumps(json_object), Key=s3_key, Bucket=self.raw_bucket
        )
        return

    def _get_landing_data(self, key, bucket):
        self.logger.debug(f"Getting {key=} from {bucket=}")
        s3_obj = self.s3.get_object(Key=key, Bucket=bucket)
        return json.loads(s3_obj["Body"].read())

    def _get_api_details(self, endpoint):
        api_table = self.dynamodb.Table(self.api_details_table)
        return api_table.get_item(
            Key={"endpoint": endpoint},
        )["Item"]

    def start_preprocessing(self, event):
        self.logger.info("Starting lambda execution...")
        s3_key = urllib.parse.unquote_plus(
            event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
        )
        bucket = urllib.parse.unquote_plus(
            event["Records"][0]["s3"]["bucket"]["name"], encoding="utf-8"
        )
        landing_json = self._get_landing_data(s3_key, bucket)

        endpoint = _get_endpoint_from_key(s3_key)
        preprocessing_details = self._get_api_details(endpoint)
        filename = preprocessing_details["s3_filename"]

        processed_json = _process_top_data(landing_json, preprocessing_details)
        self._put_to_s3_raw(processed_json, endpoint, filename)
        self.logger.info("Finished lambda execution.")
        return "SUCCESS"


def lambda_handler(event, context):
    return SpotifyDataPreprocessor().start_preprocessing(event)

import os
import json
import logging
import urllib.parse
from datetime import datetime
import boto3

LOGGER = logging.getLogger("SpotifyDataPreprocessor")
LOGGER.setLevel(logging.DEBUG)


class SpotifyDataPreprocessor:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.raw_bucket = os.getenv("RAW_BUCKET_NAME")
        self.s3 = boto3.client('s3')

    def process_top_data(self, json_object):
        spotify_items = json_object['items']
        new_json = {}
        # TODO: make a dataclass, and display this like {{'artist': , 'track': }, {},...}
        for i, item in enumerate(spotify_items):
            new_json.update({i + 1: {item["name"]: item["artists"][0]["name"]}})
        return new_json


    def put_to_s3_raw(self, json_object):
        datetime_now = datetime.now().strftime("%Y%m%d")
        # TODO: partition this better
        s3_key = f"spotify_data/top_tracks{datetime_now}.json"
        self.logger.debug(f"Putting {s3_key=} into bucket {self.raw_bucket}")
        self.s3.put_object(Body=json.dumps(json_object),
                                  Key=s3_key,
                                  Bucket=self.raw_bucket)
        return

    def get_landing_data(self, key, bucket):
        self.logger.debug(f"Getting {key=} from {bucket=}")
        s3_obj = self.s3.get_object(Key=key, Bucket=bucket)
        return json.loads(s3_obj['Body'].read())


    def start_preprocessing(self, event):
        self.logger.info("Starting lambda execution...")
        s3_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        bucket = urllib.parse.unquote_plus(event['Records'][0]['s3']['bucket']['name'], encoding='utf-8')
        landing_json = self.get_landing_data(s3_key, bucket)
        processed_json = self.process_top_data(landing_json)
        self.put_to_s3_raw(processed_json)
        self.logger.info("Finished lambda execution.")
        return "SUCCESS"


def lambda_handler(event, context):
    return SpotifyDataPreprocessor().start_preprocessing(event)

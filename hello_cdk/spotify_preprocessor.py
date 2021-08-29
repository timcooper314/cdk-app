import os
import json
import logging
import urllib.parse
from datetime import datetime
import boto3

LOGGER = logging.getLogger("SpotifyDataPreprocessor")
LOGGER.setLevel(logging.DEBUG)

# TODO: refactor into classes

def spotify_data_preprocessor(json_object):
    # TODO: Make a test for this
    spotify_items = json_object['items']
    new_json = {}
    # TODO: make a dataclass, and display this like {{'artist': , 'track': }, {},...}
    for i, item in enumerate(spotify_items):
        new_json.update({i + 1: {item["name"]: item["artists"][0]["name"]}})
    return new_json


def put_to_s3_raw(json_object):
    raw_bucket = os.getenv("RAW_BUCKET_NAME")
    s3_resource = boto3.resource('s3')
    s3_bucket = s3_resource.Bucket(raw_bucket)
    datetime_now = datetime.now().strftime("%Y%m%d")
    # TODO: partition this better
    s3_key = f"spotify_data/top_tracks{datetime_now}.json"
    LOGGER.debug(f"Putting {s3_key=} into bucket {raw_bucket}")
    s3_bucket.put_object(Key=s3_key,
                         Body=json.dumps(json_object))
    return


def get_landing_data(key, bucket):
    LOGGER.debug(f"Getting {key=} from {bucket=}")
    s3_client = boto3.client('s3')
    s3_obj = s3_client.get_object(Key=key, Bucket=bucket)
    return json.loads(s3_obj['Body'].read())


def start_preprocessing(event):
    LOGGER.info("Starting lambda execution...")
    s3_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    bucket = urllib.parse.unquote_plus(event['Records'][0]['s3']['bucket']['name'], encoding='utf-8')
    landing_json = get_landing_data(s3_key, bucket)
    processed_json = spotify_data_preprocessor(landing_json)
    put_to_s3_raw(processed_json)
    LOGGER.info("Finished lambda execution.")
    return "SUCCESS"


def lambda_handler(event, context):
    return start_preprocessing(event)

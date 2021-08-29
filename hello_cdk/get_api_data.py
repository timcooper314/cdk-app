import os
import json
from typing import List, Dict
from datetime import datetime
import requests
import boto3


def put_to_s3_landing(json_object):
    landing_bucket = os.getenv("LANDING_BUCKET_NAME")
    s3_bucket = boto3.resource('s3').Bucket(landing_bucket)
    datetime_now = datetime.now().strftime("%Y%m%d")
    s3_bucket.put_object(Key=f"spotify_data/top_tracks{datetime_now}.json",
                         Body=json.dumps(json_object))


def get_api_data():
    base_url = "https://api.spotify.com/v1/"
    endpoint = f"me/top/tracks"
    params = {"time_range": "short_term",
              "limit": 10,
              "offset": 0}
    auth_token = os.getenv("AUTH_TOKEN")
    headers = {"Authorization": f"Bearer {auth_token}"}
    response = requests.get(
        f"{base_url}{endpoint}",
        headers=headers,
        params=params,
    )
    return response.json()


def get_top_tracks_data():
    json_response = get_api_data()
    put_to_s3_landing(json_response)
    return "SUCCESS"


def lambda_handler(event, context):
    return get_top_tracks_data()

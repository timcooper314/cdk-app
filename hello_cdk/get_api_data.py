import os
import json
from datetime import datetime
import urllib3
import boto3


def put_to_s3_landing(json_object):
    landing_bucket = os.getenv("LANDING_BUCKET_NAME")
    print(landing_bucket)
    s3_resource = boto3.resource('s3')
    s3_bucket = s3_resource.Bucket(landing_bucket)
    datetime_now = datetime.now().strftime("%Y%m%d")
    s3_bucket.put_object(Key=f"spotify_data/top_tracks{datetime_now}.json",
                         Body=json.dumps(json_object))
    return


def _get_api_data():
    base_url = "https://api.spotify.com/v1/"
    endpoint = f"me/top/tracks"
    params = {"time_range": "short_term",
              "limit": 10,
              "offset": 0}
    auth_token = os.getenv("AUTH_TOKEN")
    headers = {"Authorization": f"Bearer {auth_token}"}
    http = urllib3.PoolManager()
    response = http.request('GET',
                            f"{base_url}{endpoint}",
                            headers=headers,
                            fields=params)
    return json.loads(response.data.decode('utf8'))


def get_top_tracks_data():
    json_response = _get_api_data()
    put_to_s3_landing(json_response)
    return "SUCCESS"


def lambda_handler(event, context):
    return get_top_tracks_data()

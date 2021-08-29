import os
import json
import logging
from datetime import datetime
import urllib3
import boto3

LOGGER = logging.getLogger("SpotifyApiIngestion")
LOGGER.setLevel(logging.DEBUG)


class SpotifyClientAuthTokenExpiredException(Exception):
    pass


def _check_api_response(response):
    if "error" in response:
        LOGGER.warning(f"Auth token expiry error: {response['error']}")
        raise SpotifyClientAuthTokenExpiredException(response["error"]["message"])


def put_to_s3_landing(json_object):
    landing_bucket = os.getenv("LANDING_BUCKET_NAME")
    s3_resource = boto3.resource('s3')
    s3_bucket = s3_resource.Bucket(landing_bucket)
    datetime_now = datetime.now().strftime("%Y%m%d")
    s3_key = f"spotify_data/top_tracks{datetime_now}.json"
    LOGGER.debug(f"Putting {s3_key=} into bucket {landing_bucket}")
    s3_bucket.put_object(Key=s3_key,
                         Body=json.dumps(json_object))
    return


def _get_api_data():
    base_url = "https://api.spotify.com/v1/"
    endpoint = f"me/top/tracks"
    params = {"time_range": "short_term",
              "limit": 10,
              "offset": 0}
    auth_token = "BQDgHRg2EjvdV7PcPpk9lDO5ID3YfB5-Xuei0AnW4hHMuEspUXgZTtDv-GNVyLJVR8jcJAOOtIG62BjEwl66eMSM7p6S7Ckh6ERmym4H9unY6iygMY1vdlbekjjc1w4wCr7z19Zh-15nfjmO7q9Xj9XJ68DUCYTZNunPBVpjLQnQNTq9vkU7zQm20VnjN44JOR73wcJyAc3dhLkJQ486L7bHBDykowR_u86JWWgkHYIHvzXmdwVYMez3Wg"  # os.getenv("AUTH_TOKEN")
    headers = {"Authorization": f"Bearer {auth_token}"}
    http = urllib3.PoolManager()
    LOGGER.debug("Getting API data...")
    response = http.request('GET',
                            f"{base_url}{endpoint}",
                            headers=headers,
                            fields=params)
    _check_api_response(response)
    return json.loads(response.data.decode('utf8'))


def get_top_tracks_data():
    LOGGER.info("Starting lambda execution...")
    json_response = _get_api_data()
    put_to_s3_landing(json_response)
    LOGGER.info("Finished lambda execution.")
    return "SUCCESS"


def lambda_handler(event, context):
    return get_top_tracks_data()
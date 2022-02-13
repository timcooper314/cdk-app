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


class SpotifyApiIngestion:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.landing_bucket = os.getenv("LANDING_BUCKET_NAME")
        self.s3_bucket = boto3.resource("s3").Bucket(self.landing_bucket)
        self.secret_manager = boto3.client("secretsmanager")
        self.secret_name = os.getenv("API_SECRET_NAME")
        self.datetime_now = datetime.now().strftime("%Y%m%d")
        self.http = urllib3.PoolManager()
        self.auth_token = self._get_secret_auth_token()

    def _create_s3_key(self, endpoint, time_frame):
        return (
            f"spotify/{endpoint}/{time_frame}/top_{endpoint}_{self.datetime_now}.json"
        )

    def _put_to_s3_landing(self, json_object, s3_key):
        self.logger.debug(f"Putting {s3_key=} into bucket {self.landing_bucket}")
        self.s3_bucket.put_object(Key=s3_key, Body=json.dumps(json_object))
        return

    def _get_secret_auth_token(self):
        self.logger.debug("Getting auth token from secrets manager...")
        secret_obj = self.secret_manager.get_secret_value(SecretId=self.secret_name)
        return json.loads(secret_obj["SecretString"])["SPOTIFY_AUTH_TOKEN"]

    def _get_api_data(self, endpoint, time_range):
        base_url = "https://api.spotify.com/v1/"
        url_endpoint = f"me/top/{endpoint}"
        self.logger.debug(f"Getting API data for {endpoint=}...")
        headers = {"Authorization": f"Bearer {self.auth_token}"}
        params = {"time_range": time_range, "limit": 50, "offset": 0}
        self.logger.debug(
            f"Fetching API response from {base_url}{url_endpoint} for {time_range=}..."
        )
        response = self.http.request(
            "GET", f"{base_url}{url_endpoint}", headers=headers, fields=params
        )
        json_response = json.loads(response.data.decode("utf8"))
        _check_api_response(json_response)
        return json_response

    def get_top_data(self, event):
        self.logger.info("Starting lambda execution...")
        endpoint = event.get("endpoint", "tracks")
        for time_frame in ["short_term", "medium_term", "long_term"]:
            json_response = self._get_api_data(endpoint, time_frame)
            s3_key = self._create_s3_key(endpoint, time_frame)
            self._put_to_s3_landing(json_response, s3_key)
        self.logger.info("Finished lambda execution.")
        return "SUCCESS"


def lambda_handler(event, context):
    return SpotifyApiIngestion().get_top_data(event)

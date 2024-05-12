import os
import json
import logging
import base64
from datetime import datetime
import requests
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


def base64_convert_message(message: str) -> str:
    message_bytes = message.encode("ascii")
    base64_bytes = base64.b64encode(message_bytes)
    return base64_bytes.decode("ascii")


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

    def _create_s3_key(self, endpoint, time_frame):
        return (
            f"spotify/{endpoint}/{time_frame}/top_{endpoint}_{self.datetime_now}.json"
        )

    def _put_to_s3_landing(self, json_object, s3_key):
        self.logger.debug(f"Putting {s3_key=} into bucket {self.landing_bucket}")
        self.s3_bucket.put_object(Key=s3_key, Body=json.dumps(json_object))
        return

    def _get_client_secret(self) -> dict:
        self.logger.debug(
            "Getting API client details and refresh token from secrets manager..."
        )
        secret_obj = self.secret_manager.get_secret_value(SecretId=self.secret_name)
        return json.loads(secret_obj["SecretString"])

    def _get_auth_token(self) -> str:
        self.logger.debug("Retrieving auth token...")
        client_secrets = self._get_client_secret()
        client_secret_message = (
            f"{client_secrets['CLIENT_ID']}:{client_secrets['CLIENT_SECRET']}"
        )
        header_content = base64_convert_message(client_secret_message)
        headers = {"Authorization": f"Basic {header_content}"}
        refresh_token = client_secrets["REFRESH_TOKEN"]
        data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
        response = requests.post(
            "https://accounts.spotify.com/api/token",
            headers=headers,
            data=data,
        )
        return response.json()["access_token"]

    def _get_api_data(self, endpoint, time_range):
        base_url = "https://api.spotify.com/v1/"
        url_endpoint = f"me/top/{endpoint}"
        self.logger.debug(f"Getting API data for {endpoint=}...")
        auth_token = self._get_auth_token()
        headers = {"Authorization": f"Bearer {auth_token}"}
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

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
        self.s3_bucket = boto3.resource('s3').Bucket(self.landing_bucket)
        self.secret_manager = boto3.client('secretsmanager')
        self.secret_name = "test/spotify/auth_token"

    def _put_to_s3_landing(self, json_object):
        datetime_now = datetime.now().strftime("%Y%m%d")
        s3_key = f"spotify/top_tracks{datetime_now}.json"
        self.logger.debug(f"Putting {s3_key=} into bucket {self.landing_bucket}")
        self.s3_bucket.put_object(Key=s3_key,
                                  Body=json.dumps(json_object))
        return

    def _get_api_data(self):
        base_url = "https://api.spotify.com/v1/"
        endpoint = f"me/top/tracks"
        params = {"time_range": "short_term",
                  "limit": 10,
                  "offset": 0}
        auth_token = secret_manager.get_secret_value(
            SecretId=self.secret_name
        )  # "BQDx0Q0gy86jPdAeESB2rqjLS3VfUH1pPTPjoSKnVMdVdI8ipquDksZDvUVl5ODdj9YYd9ulBzQVVENcmzoYS2--_5JEcOtuWaeXIRCy91L1pBHbEjjiZho8xidfjUwWUkcksy3VtQUmg2JGY9zq3ztiCmlU6Eeah5yl2mJFZkvznBWsYRwQ5gff7dDL0JcwOgeRA8i9Vxnvxg3noRXfgu71faHpyBSwFQQAwHIuI58w958TUE6Ln8K07w"  # os.getenv("AUTH_TOKEN")
        headers = {"Authorization": f"Bearer {auth_token}"}
        http = urllib3.PoolManager()
        self.logger.debug("Getting API data...")
        response = http.request('GET',
                                f"{base_url}{endpoint}",
                                headers=headers,
                                fields=params)
        json_response = json.loads(response.data.decode('utf8'))
        _check_api_response(json_response)
        return json_response

    def get_top_tracks_data(self):
        self.logger.info("Starting lambda execution...")
        json_response = self._get_api_data()
        self._put_to_s3_landing(json_response)
        self.logger.info("Finished lambda execution.")
        return "SUCCESS"


def lambda_handler(event, context):
    return SpotifyApiIngestion().get_top_tracks_data()

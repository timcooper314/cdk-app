import os
import json
import base64
from typing import List, Dict
from datetime import datetime, timedelta
import boto3
import requests
import urllib3

SPOTIFY_DATA_BUCKET = os.environ.get("SPOTIFY_DATA_BUCKET", "dev-spotify-landing-data")
TRACKS_TO_COLLECT = 10
SPOTIFY_USER_ID = ""

s3_client = boto3.client("s3")
secret_manager = boto3.client("secretsmanager")
http = urllib3.PoolManager()


def base64_convert_message(message: str) -> str:
    message_bytes = message.encode("ascii")
    base64_bytes = base64.b64encode(message_bytes)
    return base64_bytes.decode("ascii")


def get_client_secret() -> dict:
    print("Getting API client details and refresh token from secrets manager...")
    secret_obj = secret_manager.get_secret_value(SecretId=self.secret_name)
    return json.loads(secret_obj["SecretString"])


def get_auth_token() -> str:
    print("Retrieving auth token...")
    client_secrets = get_client_secret()
    client_secret_message = (
        f"{client_secrets['CLIENT_ID']}:{client_secrets['CLIENT_SECRET']}"
    )
    header_content = base64_convert_message(client_secret_message)
    headers = {"Authorization": f"Basic {header_content}"}
    refresh_token = client_secrets["REFRESH_TOKEN"]
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    response = requests.post(
        "https://accounts.spotify.com/api/token",
        headers=headers,
        data=data,
    )
    return response.json()["access_token"]


def get_api_data(endpoint, params={}):
    base_url = "https://api.spotify.com/v1/"
    print(f"Getting API data for {endpoint=}...")
    auth_token = get_auth_token()
    headers = {"Authorization": f"Bearer {auth_token}"}
    print(f"Fetching API response from {base_url}{endpoint}...")
    response = http.request(
        "GET", f"{base_url}{endpoint}", headers=headers, fields=params
    )
    json_response = json.loads(response.data.decode("utf8"))
    return json_response


def post_api_data(endpoint, data={}):
    base_url = "https://api.spotify.com/v1/"
    print(f"Posting API data for {endpoint=} with data {data}...")
    auth_token = get_auth_token()
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
    }
    print(f"Posting API data to {base_url}{endpoint}...")
    response = requests.post(
        f"{base_url}{endpoint}",
        headers=headers,
        data=data,
    )
    return response.json()


def get_landing_data(key, bucket) -> dict:
    print(f"Getting {key=} from {bucket=}")
    s3_obj = s3_client.get_object(Key=key, Bucket=bucket)
    return json.loads(s3_obj["Body"].read())


def get_high_rotation_playlist_id(response: dict, target_pl_name: str):
    print(f"Checking playlist exists within {len(response['items'])} items...")
    for pl_info in response["items"]:
        if pl_info["name"] == target_pl_name:
            return pl_info["id"]


def update_high_rotation_playlist(event):
    """"""
    high_rotation_playlist_name = f"{datetime.now():%Y} high ω"
    # Check playlist exists
    response = get_api_data("me/playlists")
    playlist_id = get_high_rotation_playlist_id(response, high_rotation_playlist_name)
    if not playlist_id:  # HNY
        print(f"Creating new playlist {high_rotation_playlist_name}...")
        post_response = post_api_data(
            f"users/{SPOTIFY_USER_ID}/playlists",
            json.dumps(
                {
                    "name": high_rotation_playlist_name,
                    "description": f"On high rotation in {datetime.now():%Y}",
                    "public": True,
                }
            ),
        )
        playlist_id = post_response["id"]
        existing_track_uris = []
    else:  # Get existing tracks
        print("Playlist already exists!")
        response = get_api_data(f"playlists/{playlist_id}/tracks")
        existing_track_uris = [
            track_info["track"]["uri"] for track_info in response["items"]
        ]
    # Use latest data to add any new tracks to playlist
    today_s3_key = f"spotify/tracks/short_term/top_tracks_{datetime.now() - timedelta(days=1):%Y%m%d}.json"

    today_landing_data = get_landing_data(today_s3_key, SPOTIFY_DATA_BUCKET)
    top_x_track_uris = [
        track["uri"] for track in today_landing_data["items"][:TRACKS_TO_COLLECT]
    ]

    new_highly_rotatable_track_uris = list(
        set(top_x_track_uris) - set(existing_track_uris)
    )

    response = post_api_data(
        f"playlists/{playlist_id}/tracks",
        json.dumps({"uris": new_highly_rotatable_track_uris, "position": 0}),
    )
    print(response)


def lambda_handler(event, context):
    return update_high_rotation_playlist(event)

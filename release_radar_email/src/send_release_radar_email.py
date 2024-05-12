import os
import json
import base64
from typing import List, Dict
from datetime import datetime
import requests
import urllib3
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage


SENDER_EMAIL = os.environ["SENDER_EMAIL"]
SECRET_NAME = os.environ["API_SECRET_NAME"]
RELEASE_RADAR_PLAYLIST_ID = "37i9dQZEVXbmGsH8kmlJcz"

s3_client = boto3.client("s3")
ses_client = boto3.client("ses")
secret_manager = boto3.client("secretsmanager")
http = urllib3.PoolManager()


def base64_convert_message(message: str) -> str:
    message_bytes = message.encode("ascii")
    base64_bytes = base64.b64encode(message_bytes)
    return base64_bytes.decode("ascii")


def create_html_from_data(spotify_data: List[Dict[str, str]]) -> str:
    print("Creating html content from data...")
    html_data = ""
    html_track_template = """<h3>{Artist} - {Album}</h3>
                            <p>{TotalTracks} tracks</p>
                            <p>{ReleaseDate}</p>
                            <img src=”cid:{AlbumCid}”>"""
    for track in spotify_data:
        track_html = (
            html_track_template.replace("{Artist}", track["artist"])
            .replace("{Album}", track["album"])
            .replace("{TotalTracks}", str(track["total_tracks"]))
            .replace("{ReleaseDate}", track["release_date"])
            .replace("{ImageUrl}", track["album_image"]["url"])
            .replace("{AlbumCid}", track["album"].replace(" ", ""))
        )
        html_data += track_html
    return f"<html><body><h2>Spotify Release Radar</h2>{html_data}</body></html>"


def create_email_message(html_content: str, to_email: str) -> MIMEMultipart:
    html_mime = MIMEText(html_content.encode("utf-8"), "html", "utf-8")
    msg_alt = MIMEMultipart("alternative")
    msg_alt.attach(html_mime)
    message = MIMEMultipart("related")
    message.attach(html_mime)
    message["Subject"] = "Spotify Top Data Recap!"
    message["To"] = to_email
    message.attach(msg_alt)
    return message


def send_ses_message(message: MIMEMultipart):
    print("Sending SES email...")
    ses_client.send_raw_email(
        Source=SENDER_EMAIL,
        RawMessage={"Data": message.as_string()},
    )


def _get_client_secret() -> dict:
    print("Getting API client details and refresh token from secrets manager...")
    secret_obj = secret_manager.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(secret_obj["SecretString"])


def _get_auth_token() -> str:
    print("Retrieving auth token...")
    client_secrets = _get_client_secret()
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


def get_api_data(endpoint: str):
    base_url = "https://api.spotify.com/v1/"
    print(f"Getting API data for {endpoint=}...")
    auth_token = _get_auth_token()
    headers = {"Authorization": f"Bearer {auth_token}"}
    print(f"Fetching API response from {base_url}{endpoint}...")
    response = http.request(
        "GET",
        f"{base_url}{endpoint}",
        headers=headers,
    )
    json_response = json.loads(response.data.decode("utf8"))
    return json_response


def send_release_radar_email(event):
    """Retrieves the weekly release radar, and determines new albums"""
    user_name = event.get("userName", "spotify")
    target_email = event.get("targetEmail", "timcooper314@gmail.com")
    endpoint = f"playlists/{RELEASE_RADAR_PLAYLIST_ID}/tracks"
    json_response = get_api_data(endpoint)
    new_tracks = json_response["items"]
    tracks_on_fresh_albums = []
    for item in new_tracks:
        track = item["track"]
        if (
            track["album"]["album_type"] != "album"
            and track["album"]["total_tracks"] <= 4
        ):
            continue
        elif track["album"]["name"] in [t["album"] for t in tracks_on_fresh_albums]:
            continue
        tracks_on_fresh_albums.append(
            {
                "track": track["name"],
                "artist": ", ".join(
                    [artist["name"] for artist in track["album"]["artists"]]
                ),
                "album": track["album"]["name"],
                "album_type": track["album"]["album_type"],
                "release_date": track["album"]["release_date"],
                "total_tracks": track["album"]["total_tracks"],
                "album_image": track["album"]["images"][1],
            }
        )
    print(tracks_on_fresh_albums)
    print(len(tracks_on_fresh_albums))
    html_content = create_html_from_data(tracks_on_fresh_albums)
    email_message = create_email_message(html_content, target_email)

    for album in tracks_on_fresh_albums:
        album_name = album["album"]
        image_url = album["album_image"]["url"]

        img_data = requests.get(image_url).content
        local_image_path = f"/tmp/{album_name}.jpg"
        with open(local_image_path, "wb") as handler:
            handler.write(img_data)

        img = MIMEImage(open(local_image_path, "rb").read())
        img.add_header("Content-ID", f"<{album_name.replace(' ', '')}>")
        img.add_header("Content-Disposition", "inline", filename=album_name)

        email_message.attach(img)

    send_ses_message(email_message)


def lambda_handler(event, context):
    return send_release_radar_email(event)

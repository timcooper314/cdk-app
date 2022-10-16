import os
import json
from typing import List, Dict
from datetime import datetime
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


SPOTIFY_DATA_BUCKET = os.environ["SPOTIFY_DATA_BUCKET"]
SENDER_EMAIL = os.environ["SENDER_EMAIL"]
TRACKS_TO_DISPLAY = 10
s3_client = boto3.client("s3")
ses_client = boto3.client("ses")


def create_html_from_data(spotify_data: Dict) -> str:
    print("Creating html content from data...")
    data_vals: List[Dict] = list(spotify_data.values())[:TRACKS_TO_DISPLAY]
    artist_tracks_list: List[str] = [
        f"{list(track.keys())[0]} - {list(track.values())[0]}" for track in data_vals
    ]
    html_table = """<table><tr><th>#</th><th>Track - Artist</th></tr>"""
    for track_num, track_artist in enumerate(artist_tracks_list, 1):
        html_table += f"""<tr><td>{track_num}</td><td>{track_artist}</td></tr>"""
    html_table += "</table>"
    return f"<html><body><h2>Spotify Top Tracks Recap!</h2>{html_table}</body></html>"


def get_latest_s3_key(s3_prefix: str) -> str:
    print(f"Getting S3 objects in {SPOTIFY_DATA_BUCKET} for {s3_prefix=}...")
    s3_objects = s3_client.list_objects_v2(Bucket=SPOTIFY_DATA_BUCKET, Prefix=s3_prefix)
    return s3_objects["Contents"][-1]["Key"]


def get_latest_data_from_s3(s3_prefix: str) -> dict:
    s3_key = get_latest_s3_key(s3_prefix)
    print(f"Getting {s3_key=} from s3...")
    s3_obj = s3_client.get_object(
        Bucket=SPOTIFY_DATA_BUCKET,
        Key=s3_key,
    )
    return json.loads(s3_obj["Body"].read())


def create_email_message(html_content: str, to_email: str) -> MIMEMultipart:
    html_mime = MIMEText(html_content.encode("utf-8"), "html", "utf-8")
    message_body = MIMEMultipart("alternative")
    message_body.attach(html_mime)
    message = MIMEMultipart("mixed")
    message["Subject"] = "Spotify Top Data Recap!"
    message["To"] = to_email
    message.attach(message_body)
    return message


def send_ses_message(message: MIMEMultipart):
    print("Sending SES email...")
    ses_client.send_raw_email(
        Source=SENDER_EMAIL,
        RawMessage={"Data": message.as_string()},
    )


def send_recap_email(event):
    """Retrieves and sends the daily top spotify data"""
    # TODO: Future - include comparison to the previous day
    user_name = event.get("userName", "spotify")
    top_type = event.get("topType", "tracks")
    time_frame = event.get("timeFrame", "short_term")
    target_email = event.get("targetEmail", "timcooper314@gmail.com")
    s3_prefix = f"{user_name}/{top_type}/{time_frame}/"
    latest_data = get_latest_data_from_s3(s3_prefix)
    html_content = create_html_from_data(latest_data)
    email_message = create_email_message(html_content, target_email)
    send_ses_message(email_message)


def lambda_handler(event, context):
    return send_recap_email(event)

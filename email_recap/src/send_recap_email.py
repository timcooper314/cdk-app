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


def create_html_from_data(spotify_data: List[Dict[str, str]]) -> str:
    print("Creating html content from data...")
    html_table = """<table><tr><th>#, &Delta;</th><th>Track - Artist</th></tr>"""
    for track_data in spotify_data:
        html_table += f"""<tr><td>{track_data["Rank"]}, {track_data["RankChange"]}</td><td>{track_data["Track - Artist"]}</td></tr>"""
    html_table += "</table>"
    return f"<html><body><h2>Spotify Top Tracks Recap!</h2>{html_table}</body></html>"


def transform_data(
    spotify_data_current: Dict[str, Dict], spotify_data_prev: Dict[str, Dict]
) -> List[Dict]:
    artist_track_num_map_prev = {
        list(track.keys())[0]: prev_rank
        for prev_rank, track in spotify_data_prev.items()
    }
    transformed = []
    for current_rank, track_artist_dict in spotify_data_current.items():
        track_name = list(track_artist_dict.keys())[0]
        artist_name = list(track_artist_dict.values())[0]
        previous_rank = artist_track_num_map_prev.get(track_name, 100)
        transformed.append(
            {
                "Track - Artist": f"{track_name} - {artist_name}",
                "Rank": current_rank,
                "RankChange": get_rank_change(previous_rank, current_rank),
            }
        )
    return transformed[:TRACKS_TO_DISPLAY]


def get_rank_change(previous_rank: str, current_rank: str) -> str:
    rank_delta = int(previous_rank) - int(current_rank)
    if abs(rank_delta) > 50:
        return "***"
    elif rank_delta == 0:
        return "-"
    elif rank_delta > 0:
        return f"+{str(rank_delta)}"
    return str(rank_delta)


def list_s3_objects(s3_prefix: str) -> List:
    print(f"Getting S3 objects in {SPOTIFY_DATA_BUCKET} for {s3_prefix=}...")
    s3_objects = s3_client.list_objects_v2(Bucket=SPOTIFY_DATA_BUCKET, Prefix=s3_prefix)
    return s3_objects["Contents"]


def get_data_from_s3(s3_key: str) -> dict:
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

    s3_contents = list_s3_objects(s3_prefix)
    latest_s3_key = s3_contents[-1]["Key"]
    latest_data = get_data_from_s3(latest_s3_key)

    previous_s3_key = s3_contents[-2]["Key"]
    prev_data = get_data_from_s3(previous_s3_key)

    transformed_data = transform_data(latest_data, prev_data)
    html_content = create_html_from_data(transformed_data)

    email_message = create_email_message(html_content, target_email)
    send_ses_message(email_message)


def lambda_handler(event, context):
    return send_recap_email(event)

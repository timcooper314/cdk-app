import os
import json
from datetime import datetime
import boto3

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
SPOTIFY_DATA_BUCKET = os.environ["SPOTIFY_DATA_BUCKET"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


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


def send_sns_message(message: dict):
    print("Sending SNS message...")
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=json.dumps({"default": json.dumps(message)}),
        MessageStructure="json",
    )


def send_recap_email(event):
    """Retrieves and sends the daily top spotify data"""
    # TODO: Future - include comparison to the previous day
    user_name = event.get("userName", "spotify")
    top_type = event.get("topType", "tracks")
    time_frame = event.get("timeFrame", "short_term")
    s3_prefix = f"{user_name}/{top_type}/{time_frame}/"
    latest_data = get_latest_data_from_s3(s3_prefix)
    data_payload = latest_data
    send_sns_message(data_payload)


def lambda_handler(event, context):
    return send_recap_email(event)

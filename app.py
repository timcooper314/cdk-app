#!/usr/bin/env python3
import os
import aws_cdk as cdk
from api_ingestion.api_ingestion_stack import ApiIngestionStack
from email_recap.email_recap_stack import TopDataRecapEmailStack
from update_high_rotation_playlist.high_rotation_playlist_stack import HighRotationPlaylistStack

STAGE = "dev"

app = cdk.App()
dev = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")
)
api_ingestion_stack = ApiIngestionStack(
    app,
    f"{STAGE}-spotify-api-ingestion",
    env=dev,
    stage=STAGE,
    component="spotify",
)

TopDataRecapEmailStack(
    app,
    f"{STAGE}-spotify-api-top-data-email-recap",
    env=dev,
    stage=STAGE,
    component="spotify",
    data_bucket_name=api_ingestion_stack.raw_bucket.bucket_name,
    sender_email="tim.cooper@thedatafoundry.com.au",
)

HighRotationPlaylistStack(
    app,
    f"{STAGE}-spotify-api-high-rotation-playlist",
    env=dev,
    stage=STAGE,
    component="spotify",
    landing_bucket_name="dev-spotify-landing-data",
    spotify_api_secret_name="dev/spotify/client_secret",
)

app.synth()

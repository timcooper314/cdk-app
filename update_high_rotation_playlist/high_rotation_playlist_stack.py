import json
from typing import List
from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    Fn,
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_lambda_event_sources as lambda_event_sources,
)
from constructs import Construct


class HighRotationPlaylistStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage: str,
        component: str,
        landing_bucket_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        update_playlist_function = lambda_.Function(
            self,
            "update-high-rotation-playlist",
            function_name=f"{stage}-{component}-update-high-rotation-playlist",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="update_high_rotation_playlist.lambda_handler",
            code=lambda_.Code.from_asset("./update_high_rotation_playlist/src/"),
            timeout=Duration.seconds(30),
            environment=dict(
                SPOTIFY_DATA_BUCKET=landing_bucket_name,
            ),
        )
        landing_bucket = s3.Bucket.from_bucket_name(
            self, "LandingDataBucket", landing_bucket_name
        )
        landing_bucket.grant_read(update_playlist_function)

        update_playlist_schedule = events.Rule(
            self,
            "update-playlist-schedule",
            rule_name=f"{stage}-{component}-update-high-rotation-playlist-schedule",
            schedule=events.Schedule.cron(minute="10", hour="18"),
            targets=[
                events_targets.LambdaFunction(
                    handler=update_playlist_function,
                )
            ],
        )

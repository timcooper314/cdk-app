from aws_cdk import core as cdk
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3_notifications as s3_nots
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as events_targets
from aws_cdk import aws_secretsmanager as secretsmanager


class ApiIngestionStack(cdk.Stack):
    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        landing_bucket = s3.Bucket(self, "landing-data")

        api_details_table = dynamodb.Table(
            self, table_name="api-details", partition_key="endpoint"
        )

        api_lambda = lambda_.Function(
            self,
            "get-api-data",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="get_api_data.lambda_handler",
            code=lambda_.Code.from_asset("./api_ingestion/"),
            environment=dict(
                LANDING_BUCKET_NAME=landing_bucket.bucket_name, AUTH_TOKEN="auth_token"
            ),
        )
        api_lambda_schedule = events.Schedule.cron(
            minute="00", hour="18"
        )  # .rate(cdk.Duration.days(2))
        event_lambda_tracks_target = events_targets.LambdaFunction(
            handler=api_lambda, event={"endpoint": "tracks"}
        )
        event_lambda_artists_target = events_targets.LambdaFunction(
            handler=api_lambda, event={"endpoint": "artists"}
        )
        lambda_rule = events.Rule(
            self,
            "API Lambda Schedule",
            enabled=True,
            schedule=api_lambda_schedule,
            targets=[event_lambda_tracks_target, event_lambda_artists_target],
        )
        secret = secretsmanager.Secret.from_secret_attributes(
            self,
            "test/spotify/auth_token",
            secret_complete_arn="arn:aws:secretsmanager:ap-southeast-2:158795226448:secret:test/spotify/auth_token-nn3b6q",
        )
        # secret = secretsmanager.Secret(self, "test/spotify/auth_token")
        secret.grant_read(api_lambda)
        landing_bucket.grant_write(api_lambda)
        raw_bucket = s3.Bucket(self, "raw-data")
        api_data_preprocessor_lambda = lambda_.Function(
            self,
            "spotify-data-preprocessor",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="spotify_preprocessor.lambda_handler",
            code=lambda_.Code.from_asset("./api_ingestion/"),
            environment=dict(
                RAW_BUCKET_NAME=raw_bucket.bucket_name,
                API_DETAILS_TABLE=api_details_table.table_name,
            ),
        )
        landing_bucket.grant_read(api_data_preprocessor_lambda)
        landing_bucket.add_object_created_notification(
            s3_nots.LambdaDestination(api_data_preprocessor_lambda)
        )
        raw_bucket.grant_write(api_data_preprocessor_lambda)
        api_details_table.grant_read_data(api_data_preprocessor_lambda)

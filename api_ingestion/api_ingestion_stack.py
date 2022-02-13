import json
from typing import List
from aws_cdk import core as cdk
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3_notifications as s3_nots
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as events_targets
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_lambda_event_sources as lambda_event_sources


def load_api_config() -> List:
    with open("./api_ingestion/lib/api_config.json", "r") as api_config_file:
        api_config_list = json.load(api_config_file)
    return api_config_list


class ApiIngestionStack(cdk.Stack):
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        stage: str,
        component: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.landing_bucket = s3.Bucket(
            self, "landing-data", bucket_name=f"{stage}-{component}-landing-data"
        )

        api_details_table = dynamodb.Table(
            self,
            "api-details-table",
            table_name=f"{stage}-{component}-api-details",
            partition_key=dynamodb.Attribute(
                name="endpoint", type=dynamodb.AttributeType.STRING
            ),
        )

        secret = secretsmanager.Secret(self, f"{stage}/{component}/auth_token")

        api_lambda = lambda_.Function(
            self,
            "get-api-data",
            function_name=f"{stage}-{component}-ingestion",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="get_api_data.lambda_handler",
            code=lambda_.Code.from_asset("./api_ingestion/"),
            timeout=cdk.Duration.seconds(20),
            environment=dict(
                LANDING_BUCKET_NAME=self.landing_bucket.bucket_name,
                API_SECRET_NAME=secret.secret_name,
            ),
        )
        api_lambda_schedule = events.Schedule.cron(
            minute="00", hour="18"
        )  # .rate(cdk.Duration.days(2))
        event_lambda_tracks_target = events_targets.LambdaFunction(
            handler=api_lambda,
            event=events.RuleTargetInput.from_object({"endpoint": "tracks"}),
        )
        event_lambda_artists_target = events_targets.LambdaFunction(
            handler=api_lambda,
            event=events.RuleTargetInput.from_object({"endpoint": "artists"}),
        )
        lambda_rule = events.Rule(
            self,
            "API Lambda Schedule",
            enabled=True,
            schedule=api_lambda_schedule,
            targets=[event_lambda_tracks_target, event_lambda_artists_target],
            # TODO: Make a method to create this targets list based on the api configs
        )

        secret.grant_read(api_lambda)
        self.landing_bucket.grant_write(api_lambda)
        raw_bucket = s3.Bucket(
            self,
            "raw-data",
            bucket_name=f"{stage}-{component}-raw-data",
            cors=[
                s3.CorsRule(
                    allowed_methods=[s3.HttpMethods.GET],
                    allowed_origins=["*"],
                    allowed_headers=["*"],
                    max_age=3000,
                )
            ],
        )
        landing_dlq = sqs.Queue(
            self,
            "landing-dlq",
            queue_name=f"{stage}-{component}-landing-dead-letter-queue",
        )
        self.landing_queue = sqs.Queue(
            self,
            "landing-queue",
            queue_name=f"{stage}-{component}-landing-queue",
            dead_letter_queue=sqs.DeadLetterQueue(
                queue=landing_dlq, max_receive_count=2
            ),
        )

        self._add_s3_landing_events_to_queue()

        api_data_preprocessor_lambda = lambda_.Function(
            self,
            "spotify-data-preprocessor",
            function_name=f"{stage}-{component}-preprocessor",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="spotify_preprocessor.lambda_handler",
            code=lambda_.Code.from_asset("./api_ingestion/"),
            environment=dict(
                RAW_BUCKET_NAME=raw_bucket.bucket_name,
                API_DETAILS_TABLE=api_details_table.table_name,
            ),
        )

        api_data_preprocessor_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(self.landing_queue)
        )
        self.landing_bucket.grant_read(api_data_preprocessor_lambda)
        raw_bucket.grant_write(api_data_preprocessor_lambda)
        api_details_table.grant_read_data(api_data_preprocessor_lambda)

        cdk.CfnOutput(
            self,
            "dev-api-ingestion-raw-bucket-name",
            value=raw_bucket.bucket_name,
            description="The name of the S3 raw bucket.",
            export_name="dev-api-ingestion-raw-bucket-name",
        )
        cdk.CfnOutput(
            self,
            "dev-api-ingestion-raw-bucket-arn",
            value=raw_bucket.bucket_arn,
            description="The ARN of the S3 raw bucket.",
            export_name="dev-api-ingestion-raw-bucket-arn",
        )

    def _add_s3_landing_events_to_queue(self):
        """Reads the api configs dynamoDB items, to create S3 event filters,
        based on the provided prefix and suffix for S3 landing keys"""
        api_configs_list = load_api_config()
        for api_config in api_configs_list:
            self.landing_bucket.add_event_notification(
                s3.EventType.OBJECT_CREATED,
                s3_nots.SqsDestination(self.landing_queue),
                s3.NotificationKeyFilter(
                    prefix=api_config["landing_s3_prefix"],
                    suffix=api_config["landing_s3_suffix"],
                ),
            )

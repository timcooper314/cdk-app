import json
from typing import List
from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    aws_s3_notifications as s3_nots,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
    aws_lambda_event_sources as lambda_event_sources,
)
from constructs import Construct


def load_api_config() -> List:
    with open("./api_ingestion/lib/api_config.json", "r") as api_config_file:
        api_config_list = json.load(api_config_file)
    return api_config_list


class ApiIngestionStack(Stack):
    def __init__(
        self,
        scope: Construct,
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

        secret = secretsmanager.Secret(self, f"{stage}/{component}/client_secret")
        xray_layer = lambda_.LayerVersion(
            self,
            "XrayLambdaLayer",
            layer_version_name=f"{stage}-xray-sdk-layer",
            code=lambda_.Code.from_asset(
                "./api_ingestion/xray_sdk_layer/aws_xray_sdk_243.zip"
            ),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8],
            compatible_architectures=[lambda_.Architecture.X86_64],
        )
        requests_layer = lambda_.LayerVersion(
            self,
            "RequestsLambdaLayer",
            layer_version_name=f"{stage}-requests-layer",
            code=lambda_.Code.from_asset(
                "./api_ingestion/requests_layer/requests_2_27_1.zip"
            ),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8],
            compatible_architectures=[lambda_.Architecture.X86_64],
        )
        api_lambda = lambda_.Function(
            self,
            "get-api-data",
            function_name=f"{stage}-{component}-ingestion",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="get_api_data.lambda_handler",
            code=lambda_.Code.from_asset("./api_ingestion/src/"),
            timeout=Duration.seconds(20),
            environment=dict(
                LANDING_BUCKET_NAME=self.landing_bucket.bucket_name,
                API_SECRET_NAME=secret.secret_name,
            ),
            tracing=lambda_.Tracing.ACTIVE,
            layers=[xray_layer, requests_layer],
        )
        api_lambda_schedule = events.Schedule.cron(
            minute="00", hour="18"
        )  # .rate(Duration.days(2))
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
        self.raw_bucket = s3.Bucket(
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
            code=lambda_.Code.from_asset("./api_ingestion/src/"),
            environment=dict(
                RAW_BUCKET_NAME=self.raw_bucket.bucket_name,
                API_DETAILS_TABLE=api_details_table.table_name,
            ),
            tracing=lambda_.Tracing.ACTIVE,
            layers=[xray_layer],
        )

        api_data_preprocessor_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(self.landing_queue)
        )
        self.landing_bucket.grant_read(api_data_preprocessor_lambda)
        self.raw_bucket.grant_write(api_data_preprocessor_lambda)
        api_details_table.grant_read_data(api_data_preprocessor_lambda)

        CfnOutput(
            self,
            "dev-api-ingestion-raw-bucket-name",
            value=self.raw_bucket.bucket_name,
            description="The name of the S3 raw bucket.",
            export_name="dev-api-ingestion-raw-bucket-name",
        )
        CfnOutput(
            self,
            "dev-api-ingestion-raw-bucket-arn",
            value=self.raw_bucket.bucket_arn,
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

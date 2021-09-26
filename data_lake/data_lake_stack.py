from aws_cdk import core as cdk
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3_notifications as s3_nots
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk.aws_dynamodb import Attribute
from aws_cdk.aws_lambda_event_sources import SqsEventSource


class DataLakeStack(cdk.Stack):
    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.raw_bucket = s3.Bucket(self, "raw-data")

        staging_bucket = s3.Bucket(self, "staging-data")

        self.data_contracts_table = dynamodb.Table(
            self,
            "data-contracts",
            partition_key=Attribute(
                name="key_name", type=dynamodb.AttributeType.STRING
            ),
        )

        staging_lambda = lambda_.Function(
            self,
            "raw-to-staging",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="raw_to_staging.lambda_handler",
            code=lambda_.Code.from_asset("./data_lake/"),
            environment=dict(
                STAGING_BUCKET_NAME=staging_bucket.bucket_name,
                DATA_CONTRACTS_TABLE_NAME=self.data_contracts_table.table_name,
            ),
        )

        # SQS queue from raw bucket
        raw_queue = sqs.Queue(self, "raw-queue")
        self.raw_bucket.add_object_created_notification(
            s3_nots.SqsDestination(raw_queue)
        )
        staging_lambda.add_event_source(SqsEventSource(raw_queue))

        # Grant IAM permissions:
        self.raw_bucket.grant_read(staging_lambda)
        self.raw_bucket.grant_delete(staging_lambda)
        staging_bucket.grant_write(staging_lambda)
        self.data_contracts_table.grant_read_write_data(staging_lambda)

        # TODO: state machine? glue ?
        # idea: React.js front end for viewing staged spotify data, sql filtering, etc

        # Output parameters:
        cdk.CfnOutput(
            self,
            "stagingdatabucketarn",
            value=staging_bucket.bucket_arn,
            export_name="stagingdatabucketarn",
        )

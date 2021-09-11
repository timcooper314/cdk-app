from aws_cdk import core as cdk
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3_notifications as s3_nots
from aws_cdk import aws_sqs as sqs
from aws_cdk.aws_lambda_event_sources import SqsEventSource


class DataLakeStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.raw_bucket = s3.Bucket(self, "raw-data")

        staging_bucket = s3.Bucket(self, "staging-data")

        staging_lambda = lambda_.Function(self, "raw-to-staging",
                                          runtime=lambda_.Runtime.PYTHON_3_8,
                                          handler="raw_to_staging.lambda_handler",
                                          code=lambda_.Code.from_asset("./data_lake/"),
                                          environment=dict(STAGING_BUCKET_NAME=staging_bucket.bucket_name))

        raw_queue = sqs.Queue(self, "raw-queue")
        self.raw_bucket.add_object_created_notification(s3_nots.SqsDestination(raw_queue))
        staging_lambda.add_event_source(SqsEventSource(raw_queue))

        self.raw_bucket.grant_read(staging_lambda)
        self.raw_bucket.grant_delete(staging_lambda)
        staging_bucket.grant_write(staging_lambda)

        # TODO: improve key partitioning in staging
        # TODO: dynamodb table
        # TODO: state machine? glue ?
        # idea: React.js front end for viewing staged spotify data, sql filtering, etc

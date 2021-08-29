from aws_cdk import core as cdk
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3_notifications as s3_nots
# from aws_cdk import aws_events as events


class ApiIngestionStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        landing_bucket = s3.Bucket(self, "landing-data")
        api_lambda = lambda_.Function(self, "get-api-data",
                                      runtime=lambda_.Runtime.PYTHON_3_8,
                                      handler="get_api_data.lambda_handler",
                                      code=lambda_.Code.from_asset("./hello_cdk/"),
                                      environment=dict(LANDING_BUCKET_NAME=landing_bucket.bucket_name,
                                                       AUTH_TOKEN="auth_token"))
        landing_bucket.grant_write(api_lambda)
        raw_bucket = s3.Bucket(self, "raw-data")
        api_data_preprocessor_lambda = lambda_.Function(self, "spotify-data-preprocessor",
                                                        runtime=lambda_.Runtime.PYTHON_3_8,
                                                        handler="spotify_preprocessor.lambda_handler",
                                                        code=lambda_.Code.from_asset("./hello_cdk/"),
                                                        environment=dict(RAW_BUCKET_NAME=raw_bucket.bucket_name))
        landing_bucket.grant_read(api_data_preprocessor_lambda)
        landing_bucket.add_object_created_notification(s3_nots.LambdaDestination(api_data_preprocessor_lambda))
        raw_bucket.grant_write(api_data_preprocessor_lambda)

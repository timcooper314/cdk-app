from aws_cdk import core as cdk
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3_notifications as s3_nots


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

from aws_cdk import core as cdk
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_lambda as lambda_

# For consistency with other languages, `cdk` is the preferred import name for
# the CDK's core module.  The following line also imports it as `core` for use
# with examples from the CDK Developer's Guide, which are in the process of
# being updated to use `cdk`.  You may delete this import if you don't need it.
from aws_cdk import core


class ApiIngestionStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        landing_bucket = s3.Bucket(self, "landing-data")
        api_lambda = lambda_.Function(self, "get-api-data",
                                      runtime=lambda_.Runtime.PYTHON_3_8,
                                      handler="get_api_data.lambda_handler",
                                      code=lambda_.Code.from_asset("./get_api_data"),
                                      environment=dict(LANDING_BUCKET_NAME=landing_bucket.bucket_name,
                                                       AUTH_TOKEN="auth_token"))

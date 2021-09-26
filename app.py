#!/usr/bin/env python3
import os

from aws_cdk import core as cdk

from api_ingestion.api_ingestion_stack import ApiIngestionStack
from data_lake.data_lake_stack import DataLakeStack

app = cdk.App()

prod = cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION'))

dl_stack = DataLakeStack(app, "DataLakeStack", env=prod)
ingestion_stack = ApiIngestionStack(app, "ApiIngestionStack",
                                    raw_bucket=dl_stack.raw_bucket,
                                    env=prod)

app.synth()

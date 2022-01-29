#!/usr/bin/env python3
import os
from aws_cdk import core as cdk
from api_ingestion.api_ingestion_stack import ApiIngestionStack
from api_ingestion.lib.upload_api_configs import upload_api_configs_to_dynamodb


app = cdk.App()
ApiIngestionStack(
    app,
    "ApiIngestionStack",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")
    ),
)

app.synth()

# upload_api_configs_to_dynamodb()

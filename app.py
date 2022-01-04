#!/usr/bin/env python3
import os
from aws_cdk import core as cdk
from api_ingestion.api_ingestion_stack import ApiIngestionStack


app = cdk.App()
ApiIngestionStack(
    app,
    "ApiIngestionStack",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")
    ),
)

app.synth()

#!/usr/bin/env python3
import os
import aws_cdk as cdk
from api_ingestion.api_ingestion_stack import ApiIngestionStack

STAGE = "dev"

app = cdk.App()
dev = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")
)
ApiIngestionStack(
    app,
    f"{STAGE}-spotify-api-ingestion",
    env=dev,
    stage=STAGE,
    component="spotify",
)

app.synth()

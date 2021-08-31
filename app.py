#!/usr/bin/env python3
import os

from aws_cdk import core as cdk

from api_ingestion.api_ingestion_stack import ApiIngestionStack


app = cdk.App()
ApiIngestionStack(app, "ApiIngestionStack",
    # If you don't specify 'env', this stack will be environment-agnostic.
    # Account/Region-dependent features and context lookups will not work,
    # but a single synthesized template can be deployed anywhere.

    # Specialize this stack for the AWS Account
    # and Region that are implied by the current CLI configuration.

    env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),
    )

app.synth()

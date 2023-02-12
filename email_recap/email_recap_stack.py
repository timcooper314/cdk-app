import json
from typing import List
from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    Fn,
    aws_s3 as s3,
    aws_ses as ses,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_lambda_event_sources as lambda_event_sources,
)
from constructs import Construct


class TopDataRecapEmailStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage: str,
        component: str,
        data_bucket_name: str,
        sender_email: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ses_email_sender_identity = ses.EmailIdentity(
            self,
            "SenderIdentity",
            identity=ses.Identity.email(sender_email),
        )

        send_recap_function = lambda_.Function(
            self,
            "top-data-email-recap",
            function_name=f"{stage}-{component}-top-data-email-recap",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="send_recap_email.lambda_handler",
            code=lambda_.Code.from_asset("./email_recap/src/"),
            timeout=Duration.seconds(30),
            environment=dict(
                SPOTIFY_DATA_BUCKET=data_bucket_name,
                SENDER_EMAIL=sender_email,
            ),
        )
        send_recap_function.add_to_role_policy(
            iam.PolicyStatement(
                resources=["*"],  # TODO: Make specific to SES identity
                actions=["ses:SendEmail", "ses:SendRawEmail"],
            )
        )

        data_bucket = s3.Bucket.from_bucket_name(
            self, "RawDataBucket", data_bucket_name
        )
        data_bucket.grant_read(send_recap_function)

        daily_lambda_schedule = events.Rule(
            self,
            "daily-recap-email-schedule",
            rule_name=f"{stage}-{component}-daily-recap-email-schedule",
            schedule=events.Schedule.cron(minute="15", hour="18"),
            targets=[
                events_targets.LambdaFunction(
                    handler=send_recap_function,
                    event=events.RuleTargetInput.from_object(
                        {
                            "userName": "spotify",  # TODO: Parameterise these later..
                            "topType": "tracks",
                            "timeFrame": "short_term",
                            "targetEmail": "timcooper314@gmail.com",
                        }
                    ),
                )
            ],
        )

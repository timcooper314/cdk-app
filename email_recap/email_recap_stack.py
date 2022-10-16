import json
from typing import List
from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    Fn,
    aws_sns as sns,
    aws_s3 as s3,
    aws_sns_subscriptions as sns_subscriptions,
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
        email_subscriptions: List[str],
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        sns_topic = sns.Topic(
            self,
            "email-recap-topic",
            topic_name=f"{stage}-{component}-top-data-email-recap",
        )
        for email_add in email_subscriptions:
            sns_topic.add_subscription(sns_subscriptions.EmailSubscription(email_add))

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
                SNS_TOPIC_ARN=sns_topic.topic_arn,
            ),
        )
        sns_topic.grant_publish(send_recap_function)

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
                        }
                    ),
                )
            ],
        )

from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as events_targets,
)
from constructs import Construct


class ReleaseRadarEmailStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage: str,
        component: str,
        sender_email: str,  # Must exist as an SES identity
        spotify_api_secret_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        aws_wrangler_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            "AwsWranglerLayer",
            layer_version_arn="arn:aws:lambda:ap-southeast-2:336392948345:layer:AWSSDKPandas-Python312:8",
        )
        send_release_radar_email_function = lambda_.Function(
            self,
            "release-radar-email",
            function_name=f"{stage}-{component}-release-radar-email",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="send_release_radar_email.lambda_handler",
            code=lambda_.Code.from_asset("./release_radar_email/src/"),
            timeout=Duration.seconds(30),
            environment=dict(
                SENDER_EMAIL=sender_email,
                API_SECRET_NAME=spotify_api_secret_name,
            ),
            layers=[aws_wrangler_layer],
        )
        # spotify_api_secret.grant_read(send_release_radar_email_function)
        # add policy to allow secret read
        send_release_radar_email_function.add_to_role_policy(
            iam.PolicyStatement(
                resources=["*"],
                actions=["ses:SendEmail", "ses:SendRawEmail"],
            )
        )
        send_release_radar_email_function.add_to_role_policy(
            iam.PolicyStatement(
                resources=[
                    f"arn:aws:secretsmanager:ap-southeast-2:158795226448:secret:{spotify_api_secret_name}*"
                ],
                actions=[
                    "secretsmanager:GetSecretValue",
                    "secretsmanager:DescribeSecret",
                ],
            )
        )
        weekly_lambda_schedule = events.Rule(
            self,
            "send-release-radar-email-schedule",
            rule_name=f"{stage}-{component}-weekly-release-radar-email-schedule",
            schedule=events.Schedule.cron(week_day="THU", hour="21", minute="15"),
            targets=[
                events_targets.LambdaFunction(
                    handler=send_release_radar_email_function,
                    event=events.RuleTargetInput.from_object(
                        {
                            "targetEmail": "timcooper314@gmail.com",
                        }
                    ),
                )
            ],
        )

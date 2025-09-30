from aws_cdk import (
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_sns as sns,
    Duration
)
from constructs import Construct
from ..config import PROJECT_PREFIX


class SubscriptionsLambdas(Construct):
    def __init__(self, scope: Construct, id: str, subscriptions_table, notifications_topic: sns.Topic):
        super().__init__(scope, id)

        env_vars = {
            "SUBSCRIPTIONS_TABLE": subscriptions_table.table_name,
            "NOTIFICATIONS_TOPIC_ARN": notifications_topic.topic_arn,  # ✅ pass topic ARN
        }

        self.subscriptions_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}SubscriptionsLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="subscription.handler",
            code=_lambda.Code.from_asset("lambda/subscriptions"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )

        # DynamoDB permissions
        subscriptions_table.grant_read_write_data(self.subscriptions_lambda)

        # SNS permissions
        notifications_topic.grant_publish(self.subscriptions_lambda)
        self.subscriptions_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["sns:Subscribe", "sns:Unsubscribe"],
                resources=[notifications_topic.topic_arn]
            )
        )

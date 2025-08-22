from aws_cdk import (
    aws_lambda as _lambda,
    aws_iam as iam,
    Duration
)
from constructs import Construct
from ..config import PROJECT_PREFIX

class SubscriptionsLambdas(Construct):
    def __init__(self, scope: Construct, id: str, subscriptions_table):
        super().__init__(scope, id)

        env_vars = {
            "SUBSCRIPTIONS_TABLE": subscriptions_table.table_name
        }

        self.subscriptions_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}SubscriptionsLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="subscriptions.handler",
            code=_lambda.Code.from_asset("lambda/subscriptions"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )

        self.subscriptions_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:PutItem",
                    "dynamodb:GetItem",
                    "dynamodb:Query",
                    "dynamodb:UpdateItem",
                    "dynamodb:DeleteItem"
                ],
                resources=[subscriptions_table.table_arn]
            )
        )

        subscriptions_table.grant_read_write_data(self.subscriptions_lambda)

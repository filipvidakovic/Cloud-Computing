from aws_cdk import (
    aws_lambda as _lambda,
    aws_iam as iam,
    Duration
)
from constructs import Construct
from ..config import PROJECT_PREFIX


class NotificationsLambdas(Construct):
    def __init__(self, scope: Construct, id: str, subscriptions_table, notifications_table):
        super().__init__(scope, id)

        env_vars = {
            "SUBSCRIPTIONS_TABLE": subscriptions_table.table_name,
            "NOTIFICATIONS_TABLE": notifications_table.table_name
        }

        self.notify_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}NotifyUsersLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="notify_users.handler",
            code=_lambda.Code.from_asset("lambda/notifications"),
            environment=env_vars,
            timeout=Duration.seconds(15)
        )

        subscriptions_table.grant_read_data(self.notify_lambda)
        notifications_table.grant_read_write_data(self.notify_lambda)

from aws_cdk import (
    Stack,
    aws_dynamodb as dynamodb,
)
import aws_cdk as cdk
from constructs import Construct
from ..config import PROJECT_PREFIX

class SubscriptionsTableStack(Construct):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.table = dynamodb.Table(
            self,
            "UserSubscriptions",
            table_name=f"{PROJECT_PREFIX}UserSubscriptions",
            partition_key=dynamodb.Attribute(
                name="userId", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="subscriptionId", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

        self.table.add_global_secondary_index(
            index_name="SubscriptionTypeTargetIdIndex",
            partition_key=dynamodb.Attribute(
                name="subscriptionType", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="targetId", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )

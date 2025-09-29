from aws_cdk import (
    aws_dynamodb as dynamodb,
    Stack
)
from constructs import Construct
from aws_cdk import aws_cdk as cdk

class WebsocketConnectionsTable(Construct):
    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)
        self.table = dynamodb.Table(
            self, "WebsocketConnections",
            table_name="WebsocketConnections",
            partition_key=dynamodb.Attribute(
                name="connectionId",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

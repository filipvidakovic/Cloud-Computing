from aws_cdk import aws_dynamodb as dynamodb
import aws_cdk
from constructs import Construct

from projekat.config import PROJECT_PREFIX

class RatesTable(Construct):
    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)

        self.table = dynamodb.Table(
            self, f"{PROJECT_PREFIX}RatesTable",
            partition_key=dynamodb.Attribute(name="userId", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="musicId", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=aws_cdk.RemovalPolicy.DESTROY
        )
        self.table.add_global_secondary_index(
            index_name="MusicIndex",
            partition_key=dynamodb.Attribute(name="musicId", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="rating", type=dynamodb.AttributeType.NUMBER),
            projection_type=dynamodb.ProjectionType.ALL
        )
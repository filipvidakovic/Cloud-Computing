from aws_cdk import (
    # Duration,
    Duration,
    Stack,
    aws_sqs as sqs,
)
from constructs import Construct
from projekat.auth.cognito_stack import CognitoAuth

class ProjekatStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here
        cognito = CognitoAuth(self, "CognitoAuth")
        # example resource
        queue = sqs.Queue(
            self, "ProjekatQueue",
            visibility_timeout=Duration.seconds(300),
            # removal_policy=sqs.RemovalPolicy.DESTROY
        )

from aws_cdk import (
    Duration,
    Stack,
    aws_sqs as sqs,
)
from constructs import Construct
from projekat.api.api_gateway_stack import ApiGateway
from projekat.auth.auth_lambda import AuthLambdas
from projekat.auth.cognito_stack import CognitoAuth
from projekat.config import PROJECT_PREFIX

class ProjekatStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        cognito = CognitoAuth(self, f"{PROJECT_PREFIX}Cognito")
        lambdas = AuthLambdas(self, f"{PROJECT_PREFIX}Lambdas", user_pool=cognito.user_pool, user_pool_client=cognito.user_pool_client)
        ApiGateway(self, f"{PROJECT_PREFIX}ApiGateway", lambdas=lambdas)

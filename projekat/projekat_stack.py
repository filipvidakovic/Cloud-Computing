from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb as dynamodb,
    aws_sqs as sqs,
)
from constructs import Construct
from projekat.api.api_gateway_stack import ApiGateway
from projekat.artists.artists_lambdas import ArtistLambdas
from projekat.auth.auth_lambda import AuthLambdas
from projekat.auth.cognito_stack import CognitoAuth
from projekat.config import PROJECT_PREFIX

class ProjekatStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # tables
        self.artist_table = dynamodb.Table(
            self, "ArtistTable",
            partition_key=dynamodb.Attribute(
                name="artistId",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        cognito = CognitoAuth(self, f"{PROJECT_PREFIX}Cognito")
        auth_lambdas = AuthLambdas(self, f"{PROJECT_PREFIX}Lambdas", user_pool=cognito.user_pool, user_pool_client=cognito.user_pool_client)
        artist_lambdas = ArtistLambdas(self, "ArtistLambdas", artist_table=self.artist_table)
        ApiGateway(self, f"{PROJECT_PREFIX}ApiGateway", auth_lambdas=auth_lambdas, artist_lambdas=artist_lambdas)


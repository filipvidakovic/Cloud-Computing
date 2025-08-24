import aws_cdk
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
from projekat.subscriptions.subscriptions_lambdas import SubscriptionsLambdas
from projekat.config import PROJECT_PREFIX
from aws_cdk import aws_s3 as s3

from projekat.music.music_lambdas import MusicLambdas
from projekat.subscriptions.subscriptions_table import SubscriptionsTableStack


class ProjekatStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.music_bucket = s3.Bucket(
            self, "MusicBucket",
            removal_policy=aws_cdk.RemovalPolicy.DESTROY,  # optional for dev/testing
            auto_delete_objects=True  # optional for dev/testing
        )

        # tables
        self.artist_table = dynamodb.Table(
            self, "ArtistTable",
            partition_key=dynamodb.Attribute(
                name="artistId",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )
        self.music_table = dynamodb.Table(
            self, "MusicTable",
            partition_key=dynamodb.Attribute(
                name="genre",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="musicId",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        self.subscriptions_table = SubscriptionsTableStack(self, "SubscriptionsTable")

        cognito = CognitoAuth(self, f"{PROJECT_PREFIX}Cognito")
        auth_lambdas = AuthLambdas(self, f"{PROJECT_PREFIX}Lambdas", user_pool=cognito.user_pool, user_pool_client=cognito.user_pool_client)
        music_lambdas = MusicLambdas(self, "MusicLambdas", music_table=self.music_table, s3_bucket=self.music_bucket)
        subscription_lambdas = SubscriptionsLambdas(self, "SubscriptionLambdas", subscriptions_table=self.subscriptions_table.table)
        artist_lambdas = ArtistLambdas(self, "ArtistLambdas", artist_table=self.artist_table, delete_artist_songs_lambda=music_lambdas.delete_artist_songs_lambda)
        ApiGateway(self, f"{PROJECT_PREFIX}ApiGateway", auth_lambdas=auth_lambdas, artist_lambdas=artist_lambdas, music_lambdas=music_lambdas, subscription_lambdas=subscription_lambdas)


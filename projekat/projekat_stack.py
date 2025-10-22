import aws_cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb as dynamodb,
    aws_sqs as sqs,
)
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_sns as sns
from constructs import Construct
from projekat.api.api_gateway_stack import ApiGateway
from projekat.artists.artists_lambdas import ArtistLambdas
from projekat.auth.auth_lambda import AuthLambdas
from projekat.auth.cognito_stack import CognitoAuth
from projekat.rates.rate_lambdas import RateLambdas
from projekat.rates.rate_table import RatesTable
from projekat.subscriptions.subscriptions_lambdas import SubscriptionsLambdas
from projekat.config import PROJECT_PREFIX
from aws_cdk import aws_s3 as s3
from projekat.feed_queue_stack import FeedQueueStack
from aws_cdk import aws_iam as iam
from projekat.music.music_lambdas import MusicLambdas
from projekat.subscriptions.subscriptions_table import SubscriptionsTableStack
from projekat.transcription.transcription_stack import TranscriptionStack
from projekat.user.user_lambdas import UserLambdas


class ProjekatStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.music_bucket = s3.Bucket(
            self, "MusicBucket",
            removal_policy=aws_cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True 
        )

        self.music_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[f"{self.music_bucket.bucket_arn}/*"],
                principals=[iam.ServicePrincipal("transcribe.amazonaws.com")]
            )
        )

        self.music_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[f"{self.music_bucket.bucket_arn}/transcriptions/*"],
                principals=[iam.ServicePrincipal("transcribe.amazonaws.com")]
            )
        )
        self.music_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject", "s3:GetBucketLocation"],
                resources=[f"{self.music_bucket.bucket_arn}/transcriptions/*", self.music_bucket.bucket_arn],
                principals=[iam.ServicePrincipal("transcribe.amazonaws.com")]
            )
        )

        notifications_topic = sns.Topic(
            self, "NotificationsTopic",
            topic_name=f"{PROJECT_PREFIX}NotificationsTopic"
        )

        # tables
        self.artist_table = dynamodb.Table(
            self, "ArtistTable",
            partition_key=dynamodb.Attribute(
                name="artistId",  # Partition key
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="genre",  # Sort key
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        self.artist_table.add_global_secondary_index(
            index_name="GenreIndex",
            partition_key=dynamodb.Attribute(
                name="genre",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="artistId",
                type=dynamodb.AttributeType.STRING
            )
        )

        self.artist_info_table = dynamodb.Table(
            self, "ArtistInfoTable",
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

        self.song_table = dynamodb.Table(
            self, "SongTable",
            partition_key=dynamodb.Attribute(
                name="musicId", type=dynamodb.AttributeType.STRING
            ),
            billing_mode = dynamodb.BillingMode.PAY_PER_REQUEST
        )

        
        self.transcription = TranscriptionStack(
            self, "Transcription",
            song_bucket=self.music_bucket,
            song_table=self.song_table
        )

        self.subscriptions_table = SubscriptionsTableStack(self, "SubscriptionsTable")

        #listening history
        self.user_history_table = dynamodb.Table(
            self, "UserHistoryTable",
            partition_key=dynamodb.Attribute(
                name="userId",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )
        #feed
        self.user_feed_table = dynamodb.Table(
            self,
            "UserFeedTable",
            partition_key=dynamodb.Attribute(
                name="userId",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="musicId",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )
        recompute_queue = sqs.Queue(
            self, f"{PROJECT_PREFIX}RecomputeQueue",
            queue_name=f"{PROJECT_PREFIX.lower()}-recompute-queue.fifo",
            fifo=True,
            content_based_deduplication=False
        )

        rates_table = RatesTable(self, f"{PROJECT_PREFIX}RatesTable").table
        rate_lambdas = RateLambdas(self, f"{PROJECT_PREFIX}RateLambdas", rates_table)
        cognito = CognitoAuth(self, f"{PROJECT_PREFIX}Cognito")
        auth_lambdas = AuthLambdas(self, f"{PROJECT_PREFIX}Lambdas", user_pool=cognito.user_pool, user_pool_client=cognito.user_pool_client)
        subscription_lambdas = SubscriptionsLambdas(self, "SubscriptionLambdas", subscriptions_table=self.subscriptions_table.table, notifications_topic=notifications_topic, userpool=cognito.user_pool)
        music_lambdas = MusicLambdas(self, "MusicLambdas", 
                                     music_table=self.music_table, 
                                     song_table=self.song_table,
                                     artist_info_table=self.artist_info_table, 
                                     s3_bucket=self.music_bucket,
                                     rates_table=rates_table,
                                     subscriptions_table=self.subscriptions_table.table,
                                     cognito=cognito,
                                     notifications_topic=notifications_topic
                                     )
        artist_lambdas = ArtistLambdas(
            self, "ArtistLambdas",
            artist_table=self.artist_table,
            artist_info_table=self.artist_info_table,
            delete_artist_songs_lambda=music_lambdas.delete_artist_songs_lambda
        )
        user_lambdas = UserLambdas(
            self, "UserLambdas",
            user_history_table=self.user_history_table,
            user_feed_table=self.user_feed_table,
            user_subscriptions_table=self.subscriptions_table.table,
            user_reactions_table=rates_table,
            music_table=self.music_table,
            song_table=self.song_table,
            artist_info_table=self.artist_info_table,
        )


        ApiGateway(self, f"{PROJECT_PREFIX}ApiGateway", 
                   auth_lambdas=auth_lambdas, 
                   artist_lambdas=artist_lambdas, 
                   music_lambdas=music_lambdas, 
                   subscription_lambdas=subscription_lambdas, 
                   cognito=cognito,
                   user_lambdas=user_lambdas,
                   rate_lambdas=rate_lambdas,
                   transcription_stack=self.transcription
        )
        feed_queue_stack = FeedQueueStack(
            self, f"{PROJECT_PREFIX}FeedQueue",
            env_vars={
                "USER_FEED_TABLE": self.user_feed_table.table_name,
                "USER_HISTORY_TABLE": self.user_history_table.table_name,
                "USER_SUBSCRIPTIONS_TABLE": self.subscriptions_table.table.table_name,
                "USER_REACTIONS_TABLE": rates_table.table_name,
                "MUSIC_TABLE": self.music_table.table_name,
                "SONG_TABLE": self.song_table.table_name,
                "ARTIST_INFO_TABLE": self.artist_info_table.table_name,
            },
            producer_fns=[
                subscription_lambdas.subscriptions_lambda,
                rate_lambdas.create_rate_lambda,
                rate_lambdas.delete_rate_lambda,
                music_lambdas.upload_music_lambda,
                music_lambdas.delete_music_lambda
            ],
            user_feed_table=self.user_feed_table,
            user_history_table=self.user_history_table,
            user_subscriptions_table=self.subscriptions_table.table,
            user_reactions_table=rates_table,
            music_table=self.music_table,
            song_table=self.song_table,
            artist_info_table=self.artist_info_table,
        )


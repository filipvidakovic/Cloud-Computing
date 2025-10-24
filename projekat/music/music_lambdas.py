# music_lambdas.py
from aws_cdk import Duration
from aws_cdk import (
    aws_lambda as _lambda,
    aws_sns as sns,
)
from constructs import Construct
from ..config import PROJECT_PREFIX
from aws_cdk import aws_iam as iam


class MusicLambdas(Construct):
    """
    Exposes all music-related Lambdas, including delete_artist_songs_lambda
    which can be invoked by the Artist delete Lambda for cleanup.
    """
    def __init__(
        self,
        scope: Construct,
        id: str,
        music_table,               # DynamoDB table: MUSIC_BY_GENRE_TABLE (PK=genre, SK=musicId)
        song_table,                # DynamoDB table: SONG_TABLE (PK=musicId)
        artist_info_table,         # DynamoDB table: ARTIST_INFO_TABLE (PK=artistId)
        s3_bucket,                 # S3 bucket for audio + covers
        rates_table,               # DynamoDB table for ratings
        subscriptions_table,       # DynamoDB table for user subscriptions
        cognito,                   # CognitoAuth stack (needs user_pool + arn)
        notifications_topic: sns.ITopic,  # SNS topic for fan notifications
    ):
        super().__init__(scope, id)

        # ---------- Common env vars shared by music handlers ----------
        env_vars_common = {
            "MUSIC_BY_GENRE_TABLE": music_table.table_name,
            "SONG_TABLE": song_table.table_name,
            "ARTIST_INFO_TABLE": artist_info_table.table_name,
            "S3_BUCKET": s3_bucket.bucket_name,
            "RATES_TABLE": rates_table.table_name,
            "USER_SUBSCRIPTIONS_TABLE": subscriptions_table.table_name,
            "SUBSCRIPTIONS_TABLE": subscriptions_table.table_name,
            "NOTIFICATIONS_TOPIC_ARN": notifications_topic.topic_arn,
            "USER_POOL_ID": cognito.user_pool.user_pool_id,
            "SIGNED_URL_TTL_SECONDS": "900",
        }

        # ---------- Upload ----------
        self.upload_music_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}UploadMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="upload_music.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(30),
        )
        song_table.grant_write_data(self.upload_music_lambda)
        music_table.grant_write_data(self.upload_music_lambda)
        artist_info_table.grant_write_data(self.upload_music_lambda)
        s3_bucket.grant_put(self.upload_music_lambda)
        subscriptions_table.grant_read_data(self.upload_music_lambda)
        notifications_topic.grant_publish(self.upload_music_lambda)
        # Cognito lookups for notifying followers / ownership checks
        self.upload_music_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cognito-idp:AdminGetUser", "cognito-idp:ListUsers"],
                resources=[cognito.user_pool.user_pool_arn],
            )
        )
        # Manage SNS subscriptions if your upload flow subscribes/unsubscribes users
        self.upload_music_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["sns:Subscribe", "sns:Unsubscribe"],
                resources=[notifications_topic.topic_arn],
            )
        )

        # ---------- Get albums by genre ----------
        self.get_albums_by_genre_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetAlbumsByGenreLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_albums_by_genre.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(30),
        )
        music_table.grant_read_data(self.get_albums_by_genre_lambda)
        song_table.grant_read_data(self.get_albums_by_genre_lambda)
        s3_bucket.grant_read(self.get_albums_by_genre_lambda)

        # ---------- Get music details ----------
        self.get_music_details_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_music_details.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(10),
        )
        music_table.grant_read_data(self.get_music_details_lambda)
        song_table.grant_read_data(self.get_music_details_lambda)

        # ---------- Delete single song ----------
        self.delete_music_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}DeleteMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="delete_music.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(10),
        )
        song_table.grant_read_write_data(self.delete_music_lambda)
        music_table.grant_read_write_data(self.delete_music_lambda)
        s3_bucket.grant_delete(self.delete_music_lambda)

        # ---------- Update song ----------
        self.update_music_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}UpdateMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="update_music.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(30),
        )
        song_table.grant_read_write_data(self.update_music_lambda)
        music_table.grant_read_write_data(self.update_music_lambda)
        s3_bucket.grant_put(self.update_music_lambda)
        s3_bucket.grant_delete(self.update_music_lambda)

        # ---------- Batch get by musicIds ----------
        self.batch_get_music_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}BatchGetMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_music_batch_by_genre.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(30),
        )
        rates_table.grant_read_data(self.batch_get_music_lambda)
        music_table.grant_read_data(self.batch_get_music_lambda)
        song_table.grant_read_data(self.batch_get_music_lambda)
        s3_bucket.grant_read(self.batch_get_music_lambda)

        # ---------- Download song (presigns) ----------
        self.download_song_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}DownloadSongLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="download_song.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment={**env_vars_common},
            timeout=Duration.seconds(10),
        )
        song_table.grant_read_data(self.download_song_lambda)
        s3_bucket.grant_read(self.download_song_lambda)

        # ---------- Get all songs ----------
        self.get_all_songs_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetAllSongsLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_songs.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(30),
        )
        song_table.grant_read_data(self.get_all_songs_lambda)
        s3_bucket.grant_read(self.get_all_songs_lambda)

        # ---------- Signed GET for streaming ----------
        self.get_signed_music_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetSignedMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_music_signed.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars_common,
            timeout=Duration.seconds(10),
        )
        song_table.grant_read_data(self.get_signed_music_lambda)
        s3_bucket.grant_read(self.get_signed_music_lambda)

        # Alias
        self.signed_get_lambda = self.get_signed_music_lambda

        self.get_songs_by_artist_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetSongsByArtistLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_songs_by_artist.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment={
                "ARTIST_INFO_TABLE": artist_info_table.table_name,
                "SONG_TABLE": song_table.table_name,
                "RATES_TABLE": rates_table.table_name,
                "S3_BUCKET": s3_bucket.bucket_name,
            },
            timeout=Duration.seconds(15),
        )
        artist_info_table.grant_read_data(self.get_songs_by_artist_lambda)
        song_table.grant_read_data(self.get_songs_by_artist_lambda)
        rates_table.grant_read_data(self.get_songs_by_artist_lambda)
        s3_bucket.grant_read(self.get_songs_by_artist_lambda)

        self.delete_music_batch_by_ids_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}DeleteMusicBatchByIdsLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="delete_music_batch_by_ids.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment={
                "SONG_TABLE": song_table.table_name,
                "MUSIC_BY_GENRE_TABLE": music_table.table_name,
                "ARTIST_INFO_TABLE": artist_info_table.table_name,
            },
            timeout=Duration.seconds(60),
        )
        song_table.grant_read_write_data(self.delete_music_batch_by_ids_lambda)
        music_table.grant_read_write_data(self.delete_music_batch_by_ids_lambda)
        artist_info_table.grant_read_write_data(self.delete_music_batch_by_ids_lambda)


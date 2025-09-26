from aws_cdk import aws_lambda as _lambda
from constructs import Construct
from ..config import PROJECT_PREFIX
from aws_cdk import Duration

class MusicLambdas(Construct):
    def __init__(self, scope: Construct, id: str, music_table,artist_info_table, s3_bucket):
        super().__init__(scope, id)

        env_vars = {
            "MUSIC_TABLE": music_table.table_name,
            "ARTIST_INFO_TABLE": artist_info_table.table_name,
            "S3_BUCKET": s3_bucket.bucket_name,
        }

        # Lambda to upload music
        self.upload_music_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}UploadMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="upload_music.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars,
            timeout=Duration.seconds(30)
        )
        # Lambda to get albums by genre
        self.get_albums_by_genre_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetAlbumsByGenreLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_albums_by_genre.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars,
            timeout=Duration.seconds(30)
        )

        # Permissions
        music_table.grant_read_data(self.get_albums_by_genre_lambda)
        music_table.grant_write_data(self.upload_music_lambda)
        s3_bucket.grant_put(self.upload_music_lambda)

        # Lambda to get music metadata and URL
        self.get_music_details_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_music_details.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )
        # Permissions
        music_table.grant_read_data(self.get_music_details_lambda)

        # Lambda for deleting a song
        self.delete_music_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}DeleteMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="delete_music.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )
        # Permissions
        music_table.grant_read_write_data(self.delete_music_lambda)
        s3_bucket.grant_delete(self.delete_music_lambda)

        # Lambda for deleting all songs of an artist
        self.delete_artist_songs_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}DeleteArtistSongsLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="delete_artist_songs.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars,
            timeout=Duration.seconds(30)
        )
        music_table.grant_read_write_data(self.delete_artist_songs_lambda)
        s3_bucket.grant_delete(self.delete_artist_songs_lambda)

        # Lambda for updating music by musicId
        self.update_music_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}UpdateMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="update_music.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars,
            timeout=Duration.seconds(30)
        )
        # Permissions
        music_table.grant_read_write_data(self.update_music_lambda)
        s3_bucket.grant_put(self.update_music_lambda)
        s3_bucket.grant_delete(self.update_music_lambda)

        # Lambda to batch get music by genre+ids
        self.batch_get_music_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}BatchGetMusicLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_music_batch_by_genre.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars,
            timeout=Duration.seconds(30)
        )

        # Grant read access to DynamoDB
        music_table.grant_read_data(self.batch_get_music_lambda)
        s3_bucket.grant_read(self.batch_get_music_lambda)



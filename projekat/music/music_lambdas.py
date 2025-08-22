from aws_cdk import aws_lambda as _lambda
from constructs import Construct
from ..config import PROJECT_PREFIX
from aws_cdk import Duration

class MusicLambdas(Construct):
    def __init__(self, scope: Construct, id: str, music_table, s3_bucket):
        super().__init__(scope, id)

        env_vars = {
            "MUSIC_TABLE": music_table.table_name,
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

        # Lambda to get artists by genre
        self.get_artists_by_genre_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetArtistsByGenreLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_artists_by_genre.lambda_handler",
            code=_lambda.Code.from_asset("lambda/music"),
            environment=env_vars,
            timeout=Duration.seconds(30)
        )
        # Permissions
        music_table.grant_read_data(self.get_artists_by_genre_lambda)
        music_table.grant_read_data(self.get_albums_by_genre_lambda)
        music_table.grant_write_data(self.upload_music_lambda)
        s3_bucket.grant_put(self.upload_music_lambda)

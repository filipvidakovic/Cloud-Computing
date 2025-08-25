from aws_cdk import Duration
from aws_cdk import aws_lambda as _lambda
from constructs import Construct
from ..config import PROJECT_PREFIX

class ArtistLambdas(Construct):
    def __init__(self, scope: Construct, id: str, artist_table, delete_artist_songs_lambda):
        super().__init__(scope, id)

        env_vars = {
            "ARTISTS_TABLE": artist_table.table_name,
            "GENRE_INDEX": "GenreIndex",
        }

        self.create_artist_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}CreateArtistLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="create_artist.lambda_handler",
            code=_lambda.Code.from_asset("lambda/artists"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )

        self.get_artist_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetArtistLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_artist.lambda_handler",
            code=_lambda.Code.from_asset("lambda/artists"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )

        self.get_artists_by_genre_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetArtistsByGenreLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_artists_by_genre.lambda_handler",
            code=_lambda.Code.from_asset("lambda/artists"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )

        self.delete_artist_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}DeleteArtistLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="delete_artist.lambda_handler",
            code=_lambda.Code.from_asset("lambda/artists"),
            environment={
                "ARTISTS_TABLE": artist_table.table_name,
                "DELETE_SONGS_FUNCTION": delete_artist_songs_lambda.function_name
            },
            timeout=Duration.seconds(10)
        )

        artist_table.grant_read_data(self.get_artist_lambda)
        artist_table.grant_read_data(self.get_artists_by_genre_lambda)
        artist_table.grant_write_data(self.create_artist_lambda)
        artist_table.grant_read_write_data(self.delete_artist_lambda)
        delete_artist_songs_lambda.grant_invoke(self.delete_artist_lambda)

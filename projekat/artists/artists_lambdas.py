from aws_cdk import Duration
from aws_cdk import aws_lambda as _lambda
from constructs import Construct
from ..config import PROJECT_PREFIX

class ArtistLambdas(Construct):
    def __init__(self, scope: Construct, id: str, artist_table, artist_info_table, delete_artist_songs_lambda):
        super().__init__(scope, id)

        env_vars = {
            "ARTISTS_TABLE": artist_table.table_name,
            "ARTIST_INFO_TABLE": artist_info_table.table_name,
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

        self.update_artist_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}UpdateArtistLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="update_artist.lambda_handler",  # <-- points to lambda/artists/update_artist.py
            code=_lambda.Code.from_asset("lambda/artists"),
            environment=env_vars,
            timeout=Duration.seconds(15)
        )


        self.delete_artist_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}DeleteArtistLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="delete_artist.lambda_handler",
            code=_lambda.Code.from_asset("lambda/artists"),
            environment=env_vars,
            timeout=Duration.seconds(120),  # give it room to scan/delete
        )

        artist_table.grant_read_data(self.get_artist_lambda)
        artist_table.grant_read_data(self.get_artists_by_genre_lambda)
        artist_table.grant_write_data(self.create_artist_lambda)
        artist_table.grant_read_write_data(self.delete_artist_lambda)
        artist_table.grant_read_write_data(self.update_artist_lambda)

        artist_info_table.grant_read_data(self.get_artist_lambda)
        artist_info_table.grant_read_data(self.get_artists_by_genre_lambda)
        artist_info_table.grant_write_data(self.create_artist_lambda)
        artist_info_table.grant_read_write_data(self.delete_artist_lambda)
        artist_info_table.grant_read_write_data(self.update_artist_lambda)

        delete_artist_songs_lambda.grant_invoke(self.delete_artist_lambda)

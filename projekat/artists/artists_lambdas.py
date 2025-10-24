# artist_lambdas.py
from aws_cdk import Duration
from aws_cdk import aws_lambda as _lambda
from constructs import Construct
from ..config import PROJECT_PREFIX


class ArtistLambdas(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        artist_table,
        artist_info_table,
        song_table,               # <- NEW
        music_by_genre_table,     # <- NEW
    ):
        super().__init__(scope, id)

        # Shared env for read/write artist lambdas (no music env here)
        env_vars_common = {
            "ARTISTS_TABLE": artist_table.table_name,
            "ARTIST_INFO_TABLE": artist_info_table.table_name,
            "GENRE_INDEX": "GenreIndex",
        }

        self.create_artist_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}CreateArtistLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="create_artist.lambda_handler",
            code=_lambda.Code.from_asset("lambda/artists"),
            environment=env_vars_common,
            timeout=Duration.seconds(10),
        )

        self.get_artist_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetArtistLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_artist.lambda_handler",
            code=_lambda.Code.from_asset("lambda/artists"),
            environment=env_vars_common,
            timeout=Duration.seconds(10),
        )

        self.get_artists_by_genre_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetArtistsByGenreLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_artists_by_genre.lambda_handler",
            code=_lambda.Code.from_asset("lambda/artists"),
            environment=env_vars_common,
            timeout=Duration.seconds(10),
        )

        self.update_artist_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}UpdateArtistLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="update_artist.lambda_handler",
            code=_lambda.Code.from_asset("lambda/artists"),
            environment=env_vars_common,
            timeout=Duration.seconds(15),
        )

        # Delete artist + delete all songs referenced in artist_info.songs
        delete_env = {
            **env_vars_common,
            "SONG_TABLE": song_table.table_name,                         # <- NEW
            "MUSIC_BY_GENRE_TABLE": music_by_genre_table.table_name,     # <- NEW
        }
        self.delete_artist_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}DeleteArtistLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="delete_artist.lambda_handler",
            code=_lambda.Code.from_asset("lambda/artists"),
            environment=delete_env,
            timeout=Duration.seconds(120),
        )

        # ------- Grants -------
        # artist tables
        artist_table.grant_read_data(self.get_artist_lambda)
        artist_table.grant_read_data(self.get_artists_by_genre_lambda)
        artist_table.grant_write_data(self.create_artist_lambda)
        artist_table.grant_read_write_data(self.update_artist_lambda)
        artist_table.grant_read_write_data(self.delete_artist_lambda)

        artist_info_table.grant_read_data(self.get_artist_lambda)
        artist_info_table.grant_read_data(self.get_artists_by_genre_lambda)
        artist_info_table.grant_write_data(self.create_artist_lambda)
        artist_info_table.grant_read_write_data(self.update_artist_lambda)
        artist_info_table.grant_read_write_data(self.delete_artist_lambda)

        # music tables (needed only by delete lambda)
        song_table.grant_read_write_data(self.delete_artist_lambda)              # <- NEW
        music_by_genre_table.grant_read_write_data(self.delete_artist_lambda)    # <- NEW

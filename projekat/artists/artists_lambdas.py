from aws_cdk import aws_lambda as _lambda
from constructs import Construct
from ..config import PROJECT_PREFIX
from aws_cdk import Duration

class ArtistLambdas(Construct):
    def __init__(self, scope: Construct, id: str, artist_table, delete_artist_songs_lambda):
        super().__init__(scope, id)

        env_vars = {
            "ARTISTS_TABLE": artist_table.table_name
        }

        # Lambda to create artists
        self.create_artist_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}CreateArtistLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="create_artist.lambda_handler",
            code=_lambda.Code.from_asset("lambda/artists"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )
        # Lambda to get artist by artistId
        self.get_artist_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetArtistLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_artist.lambda_handler",
            code=_lambda.Code.from_asset("lambda/artists"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )

        # Lambda to delete an artist by artistId
        self.delete_artist_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}DeleteArtistLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="delete_artist.lambda_handler",
            code=_lambda.Code.from_asset("lambda/artists"),
            environment={"ARTISTS_TABLE": artist_table.table_name, "DELETE_SONGS_FUNCTION": delete_artist_songs_lambda.function_name},
            timeout=Duration.seconds(10)
        )

        #Permissions
        artist_table.grant_read_data(self.get_artist_lambda)
        artist_table.grant_write_data(self.create_artist_lambda)
        artist_table.grant_read_write_data(self.delete_artist_lambda)


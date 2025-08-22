from aws_cdk import aws_apigateway as apigw
from constructs import Construct
from ..config import PROJECT_PREFIX
from ..music import music_lambdas


class ApiGateway(Construct):
    def __init__(self, scope: Construct, id: str, auth_lambdas, artist_lambdas, music_lambdas):
        super().__init__(scope, id)

        #users
        api = apigw.RestApi(self, f"{PROJECT_PREFIX}AuthApi", rest_api_name=f"{PROJECT_PREFIX}CognitoAuthApi")

        api.root.add_resource("register").add_method(
            "POST", apigw.LambdaIntegration(auth_lambdas.register_lambda)
        )

        api.root.add_resource("login").add_method(
            "POST", apigw.LambdaIntegration(auth_lambdas.login_lambda)
        )

        # artists
        artists_resource = api.root.add_resource("artists")
        artists_resource.add_method(
            "POST",
            apigw.LambdaIntegration(artist_lambdas.create_artist_lambda)
        )

        # music content
        music_resource = api.root.add_resource("music")
        music_resource.add_method(
            "POST",
            apigw.LambdaIntegration(music_lambdas.upload_music_lambda)
        )

        # discover albums
        discover_resource = music_resource.add_resource("discover-albums")
        discover_resource.add_method(
            "GET",
            apigw.LambdaIntegration(music_lambdas.get_albums_by_genre_lambda)
        )

        # discover artists
        discover_artists_resource = music_resource.add_resource("discover-artists")
        discover_artists_resource.add_method(
            "GET",
            apigw.LambdaIntegration(music_lambdas.get_artists_by_genre_lambda)
        )

        # get artist
        artist_resource = artists_resource.add_resource("{artistId}")
        artist_resource.add_method(
            "GET",
            apigw.LambdaIntegration(artist_lambdas.get_artist_lambda)
        )


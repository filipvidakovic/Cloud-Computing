from aws_cdk import aws_apigateway as apigw
from constructs import Construct

from projekat.auth.cognito_stack import CognitoAuth
from ..config import PROJECT_PREFIX
from ..music import music_lambdas
import aws_cdk


class ApiGateway(Construct):
    def __init__(self, scope: Construct, id: str, auth_lambdas, artist_lambdas, music_lambdas, subscription_lambdas, cognito: CognitoAuth):
        super().__init__(scope, id)
        api = apigw.RestApi(
            self,
            f"{PROJECT_PREFIX}RESTApi",
            rest_api_name=f"{PROJECT_PREFIX}RESTApi",
            deploy_options=apigw.StageOptions(stage_name="prod"),
            cloud_watch_role=True,
            retain_deployments=False,
            default_cors_preflight_options=apigw.CorsOptions(
                allow_origins=apigw.Cors.ALL_ORIGINS,
                allow_methods=apigw.Cors.ALL_METHODS,
                allow_headers=["Content-Type", "Authorization"],
            )
        )
        authorizer = apigw.CognitoUserPoolsAuthorizer(
            self,
            "CognitoAuthorizer",
            cognito_user_pools=[cognito.user_pool]
        )
        #users
        api.root.add_resource("register").add_method(
            "POST", apigw.LambdaIntegration(auth_lambdas.register_lambda)
        )

        api.root.add_resource("login").add_method(
            "POST", apigw.LambdaIntegration(auth_lambdas.login_lambda)
        )

        user_resource = api.root.add_resource("users")
        user_resource.add_resource("{userId}").add_method(
            "GET", apigw.LambdaIntegration(auth_lambdas.get_user_lambda),
        )
        # artists

        artists_resource = api.root.add_resource("artists")
        artists_resource.add_method(
            "POST",
            apigw.LambdaIntegration(artist_lambdas.create_artist_lambda)
        )

        # get artist
        artist_resource = artists_resource.add_resource("{artistId}")
        artist_resource.add_method(
            "GET",
            apigw.LambdaIntegration(artist_lambdas.get_artist_lambda)
        )
        artists_resource.add_method(
            "DELETE",
            apigw.LambdaIntegration(artist_lambdas.delete_artist_lambda)
        )
        artists_resource.add_method(
            "GET",
            apigw.LambdaIntegration(artist_lambdas.get_artists_by_genre_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer
        )



        # music content
        music_resource = api.root.add_resource("music")
        music_resource.add_method(
            "POST",
            apigw.LambdaIntegration(music_lambdas.upload_music_lambda)
        )
        music_resource.add_method(
            "GET",
            apigw.LambdaIntegration(music_lambdas.get_music_details_lambda)
        )
        music_resource.add_method(
            "DELETE",
            apigw.LambdaIntegration(music_lambdas.delete_music_lambda)
        )
        music_resource.add_method(
            "PUT",
            apigw.LambdaIntegration(music_lambdas.update_music_lambda)
        )
        
        delete_by_artist = music_resource.add_resource("delete-by-artist").add_resource("{artistId}")
        delete_by_artist.add_method(
            "DELETE",
            apigw.LambdaIntegration(music_lambdas.delete_artist_songs_lambda)
        )

        # discover albums
        album_resource = music_resource.add_resource("albums")
        album_resource.add_method(
            "GET",
            apigw.LambdaIntegration(music_lambdas.get_albums_by_genre_lambda)
        )

        # discover artists
        discover_artists_resource = music_resource.add_resource("discover-artists")
        discover_artists_resource.add_method(
            "GET",
            apigw.LambdaIntegration(artist_lambdas.get_artists_by_genre_lambda)
        )

        # subscriptions
        subscriptions_resource = api.root.add_resource("subscriptions")
        subscriptions_resource.add_method(
            "GET",
            apigw.LambdaIntegration(subscription_lambdas.subscriptions_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )
        single_subscription = subscriptions_resource.add_resource("{subscriptionKey}")
        single_subscription.add_method(
            "POST",
            apigw.LambdaIntegration(subscription_lambdas.subscriptions_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )
        single_subscription.add_method(
            "DELETE",
            apigw.LambdaIntegration(subscription_lambdas.subscriptions_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )

# Authorization: Bearer eyJraWQiOiJVNWZyc1wvU0ZMajVyd3Y1aFRqSG5WZDZEZzMxaElnYmVMVUpzRmpYOTNwRT0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhM2E0YTg4Mi03MDUxLTcwMDctZGE1NC1iMjA2MThiZGQ1NGEiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImJpcnRoZGF0ZSI6IjIwMDItMDEtMDEiLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAuZXUtY2VudHJhbC0xLmFtYXpvbmF3cy5jb21cL2V1LWNlbnRyYWwtMV95NGR0WURrTnoiLCJjb2duaXRvOnVzZXJuYW1lIjoiZmlsaXAiLCJnaXZlbl9uYW1lIjoiRmlsaXAiLCJvcmlnaW5fanRpIjoiMDZlMzhlZTgtNGQ0OC00NmYxLTg0NmItN2VmZmY3MGU0ODY2IiwiYXVkIjoiN2gzaG83c3IzN2FhN2oxZnEwdGk4cTFvaTciLCJldmVudF9pZCI6IjcxZGM3ZmQ4LWI2ZWQtNDI3Ni05MmZlLTBhMDkxZjE5OTIzOCIsInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNzU2ODUzMzgwLCJleHAiOjE3NTY4NTY5ODAsImlhdCI6MTc1Njg1MzM4MCwiZmFtaWx5X25hbWUiOiJWaWRha292aWMiLCJqdGkiOiI0NWVlYjAzMS1hZTBlLTRjNTMtOThmNi01YzBkZDFlNTI4NWIiLCJlbWFpbCI6ImZpbGlwQGdtYWlsLmNvbSJ9.N1WqAU7CUMBYZxR_6x5LuTXYsBIxvBFngEk1SRcK5ZBZ7atvIo6J7FdlHokmqTiUBu7nIHOfKCqUKAVmt_ifPwxtakL5LBcWqLrPm1KdYwJE9hPMM48KDyWqIH1xh_3F9DvKP7ZEyGS1f2o4jMMz0oxvXR-X44cIGqbD72FK6sEzP5JY-Z40SWXGjW5TmrEszuxjItRzdn-zxtGzD4gBafhDIUtlu-_WCYKIlAxUZFJaB1p9pg7J4bPBzEZbZBMRlii2lyNBXcSJHiRAmWBsDXRvLDBsWHPs6XCSfMp1lvwtTClbmtQyyrWKzWgyeV6kpuyZ02HpYbtz1myijcttIg
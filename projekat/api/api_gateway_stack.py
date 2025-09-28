from aws_cdk import aws_apigateway as apigw
from constructs import Construct

from projekat.artists.artists_lambdas import ArtistLambdas
from projekat.auth.auth_lambda import AuthLambdas
from projekat.auth.cognito_stack import CognitoAuth
from projekat.rates.rate_lambdas import RateLambdas
from projekat.subscriptions.subscriptions_lambdas import SubscriptionsLambdas
from ..config import PROJECT_PREFIX
from ..user.user_lambdas import UserLambdas
from ..music import music_lambdas
import aws_cdk


class ApiGateway(Construct):
    def __init__(self, scope: Construct, id: str, 
                 auth_lambdas: AuthLambdas, 
                 artist_lambdas: ArtistLambdas, 
                 music_lambdas: music_lambdas.MusicLambdas, 
                 subscription_lambdas: SubscriptionsLambdas, 
                 cognito: CognitoAuth,
                 user_lambdas: UserLambdas,
                 rate_lambdas: RateLambdas):
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
        record_play_resource = api.root.add_resource("record-play")
        record_play_resource.add_method(
            "POST",
            apigw.LambdaIntegration(user_lambdas.record_play_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
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
        artist_resource.add_method(
            "DELETE",
            apigw.LambdaIntegration(artist_lambdas.delete_artist_lambda)
        )

        artists_resource.add_method(
            "GET",
            apigw.LambdaIntegration(artist_lambdas.get_artists_by_genre_lambda),
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

        # content rates
        rate_resource = api.root.add_resource("rate")
        rate_resource.add_method(
            "POST",
            apigw.LambdaIntegration(rate_lambdas.create_rate_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )
        rate_resource.add_method(
            "GET",
            apigw.LambdaIntegration(rate_lambdas.get_rate_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )
        rate_resource.add_method(
            "DELETE",
            apigw.LambdaIntegration(rate_lambdas.delete_rate_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )

        # discover albums
        album_resource = music_resource.add_resource("albums")
        album_resource.add_method(
            "GET",
            apigw.LambdaIntegration(music_lambdas.get_albums_by_genre_lambda)
        )


        # subscriptions
        subscriptions_resource = api.root.add_resource("subscriptions")
        subscriptions_resource.add_method(
            "GET",
            apigw.LambdaIntegration(subscription_lambdas.subscriptions_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )
        subscriptions_resource.add_method(
            "POST",
            apigw.LambdaIntegration(subscription_lambdas.subscriptions_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )
        single_subscription = subscriptions_resource.add_resource("{subscriptionKey}")
        single_subscription.add_method(
            "DELETE",
            apigw.LambdaIntegration(subscription_lambdas.subscriptions_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )
        # music/batchGetByIds  (POST)
        batch_get = music_resource.add_resource("batchGetByIds")
        batch_get.add_method(
            "POST",
            apigw.LambdaIntegration(music_lambdas.batch_get_music_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )

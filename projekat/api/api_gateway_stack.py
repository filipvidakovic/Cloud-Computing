from aws_cdk import aws_apigateway as apigw
from constructs import Construct

from projekat.artists.artists_lambdas import ArtistLambdas
from projekat.auth.auth_lambda import AuthLambdas
from projekat.auth.cognito_stack import CognitoAuth
from projekat.rates.rate_lambdas import RateLambdas
from projekat.subscriptions.subscriptions_lambdas import SubscriptionsLambdas
from projekat.transcription.transcription_stack import TranscriptionStack
from ..config import PROJECT_PREFIX
from ..user.user_lambdas import UserLambdas
from ..music import music_lambdas


class ApiGateway(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        auth_lambdas: AuthLambdas,
        artist_lambdas: ArtistLambdas,
        music_lambdas: music_lambdas.MusicLambdas,
        subscription_lambdas: SubscriptionsLambdas,
        cognito: CognitoAuth,
        user_lambdas: UserLambdas,
        rate_lambdas: RateLambdas,
        transcription_stack: TranscriptionStack,
    ):
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
            ),
        )

        # CORS on default 4xx/5xx from API Gateway itself
        api.add_gateway_response(
            "Default4xx",
            type=apigw.ResponseType.DEFAULT_4_XX,
            response_headers={
                "Access-Control-Allow-Origin": "'*'",
                "Access-Control-Allow-Headers": "'*'",
                "Access-Control-Allow-Methods": "'*'",
            },
        )
        api.add_gateway_response(
            "Default5xx",
            type=apigw.ResponseType.DEFAULT_5_XX,
            response_headers={
                "Access-Control-Allow-Origin": "'*'",
                "Access-Control-Allow-Headers": "'*'",
                "Access-Control-Allow-Methods": "'*'",
            },
        )

        authorizer = apigw.CognitoUserPoolsAuthorizer(
            self, "CognitoAuthorizer", cognito_user_pools=[cognito.user_pool]
        )

        # ---------- Auth ----------
        api.root.add_resource("register").add_method(
            "POST", apigw.LambdaIntegration(auth_lambdas.register_lambda)
        )
        api.root.add_resource("login").add_method(
            "POST", apigw.LambdaIntegration(auth_lambdas.login_lambda)
        )

        # ---------- Users ----------
        user_resource = api.root.add_resource("users")
        user_resource.add_resource("{userId}").add_method(
            "GET",
            apigw.LambdaIntegration(auth_lambdas.get_user_lambda),
        )
        record_play_resource = api.root.add_resource("record-play")
        record_play_resource.add_method(
            "POST",
            apigw.LambdaIntegration(user_lambdas.record_play_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )

        # ---------- Artists ----------
        artists_resource = api.root.add_resource("artists")
        artists_resource.add_method(
            "POST",
            apigw.LambdaIntegration(artist_lambdas.create_artist_lambda),
        )
        artist_resource = artists_resource.add_resource("{artistId}")
        artist_resource.add_method(
            "GET",
            apigw.LambdaIntegration(artist_lambdas.get_artist_lambda),
        )
        artist_resource.add_method(
            "DELETE",
            apigw.LambdaIntegration(artist_lambdas.delete_artist_lambda),
        )
        artists_resource.add_method(
            "GET",
            apigw.LambdaIntegration(artist_lambdas.get_artists_by_genre_lambda),
        )
        artist_resource.add_method(
            "PUT",
            apigw.LambdaIntegration(artist_lambdas.update_artist_lambda),
        )

        # ---------- Music ----------
        music_resource = api.root.add_resource("music")
        music_resource.add_method(
            "POST",
            apigw.LambdaIntegration(music_lambdas.upload_music_lambda),
        )
        music_resource.add_method(
            "GET",
            apigw.LambdaIntegration(music_lambdas.get_music_details_lambda),
        )
        music_resource.add_method(
            "DELETE",
            apigw.LambdaIntegration(music_lambdas.delete_music_lambda),
        )
        music_resource.add_method(
            "PUT",
            apigw.LambdaIntegration(music_lambdas.update_music_lambda),
        )

        # NEW: /music/by-artist/{artistId}
        by_artist = music_resource.add_resource("by-artist")
        by_artist_id = by_artist.add_resource("{artistId}")
        by_artist_id.add_method(
            "GET",
            apigw.LambdaIntegration(music_lambdas.get_songs_by_artist_lambda),
            # If you want to require login to include 'rate', uncomment:
            # authorization_type=apigw.AuthorizationType.COGNITO,
            # authorizer=authorizer,
        )

        all_songs_resource = music_resource.add_resource("all")
        all_songs_resource.add_method(
            "GET",
            apigw.LambdaIntegration(music_lambdas.get_all_songs_lambda),
        )

        # ---------- Rates ----------
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

        # ---------- Albums ----------
        album_resource = music_resource.add_resource("albums")
        album_resource.add_method(
            "GET",
            apigw.LambdaIntegration(music_lambdas.get_albums_by_genre_lambda),
        )

        # ---------- Feed ----------
        feed_resource = api.root.add_resource("feed")
        feed_resource.add_method(
            "GET",
            apigw.LambdaIntegration(user_lambdas.get_feed_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
            method_responses=[
                apigw.MethodResponse(
                    status_code="200",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True,
                        "method.response.header.Access-Control-Allow-Headers": True,
                        "method.response.header.Access-Control-Allow-Methods": True,
                    },
                ),
                apigw.MethodResponse(
                    status_code="401",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True,
                        "method.response.header.Access-Control-Allow-Headers": True,
                        "method.response.header.Access-Control-Allow-Methods": True,
                    },
                ),
                apigw.MethodResponse(
                    status_code="500",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True,
                        "method.response.header.Access-Control-Allow-Headers": True,
                        "method.response.header.Access-Control-Allow-Methods": True,
                    },
                ),
            ],
        )

        # ---------- Subscriptions ----------
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

        # ---------- Batch Get ----------
        batch_get = music_resource.add_resource("batchGetByIds")
        batch_get.add_method(
            "POST",
            apigw.LambdaIntegration(music_lambdas.batch_get_music_lambda),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )

        # ---------- Download / Signed GET ----------
        music_download = music_resource.add_resource("download")
        music_download.add_method(
            "GET",
            apigw.LambdaIntegration(music_lambdas.download_song_lambda),
        )
        signed_get = music_resource.add_resource("signedGet")
        signed_get.add_method(
            "GET",
            apigw.LambdaIntegration(music_lambdas.get_signed_music_lambda),
        )

        # ---------- Transcriptions ----------
        transcriptions_resource = api.root.add_resource("transcriptions")
        transcriptions_resource.add_resource("{songId}").add_method(
            "GET",
            apigw.LambdaIntegration(transcription_stack.process_fn),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )
        transcriptions_resource.add_resource("start").add_method(
            "POST",
            apigw.LambdaIntegration(transcription_stack.start_fn),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )

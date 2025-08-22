from aws_cdk import aws_apigateway as apigw
from constructs import Construct
from ..config import PROJECT_PREFIX
from ..music import music_lambdas


class ApiGateway(Construct):
    def __init__(self, scope: Construct, id: str, auth_lambdas, artist_lambdas, music_lambdas, subscription_lambdas):
        super().__init__(scope, id)
        api = apigw.RestApi(
            self,
            f"{PROJECT_PREFIX}RESTApi",
            rest_api_name=f"{PROJECT_PREFIX}RESTApi",
            default_cors_preflight_options=apigw.CorsOptions(
                allow_origins=apigw.Cors.ALL_ORIGINS,
                allow_methods=apigw.Cors.ALL_METHODS,
                allow_headers=["Content-Type", "Authorization"],
            )
        )
        #users
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

        # subscriptions
        subscriptions_resource = api.root.add_resource("subscriptions")
        subscriptions_resource.add_method(
            "POST",
            apigw.LambdaIntegration(subscription_lambdas.subscriptions_lambda)
        )
        subscriptions_resource.add_method(
            "GET",
            apigw.LambdaIntegration(subscription_lambdas.subscriptions_lambda)
        )
        subscriptions_resource.add_method(
            "DELETE",
            apigw.LambdaIntegration(subscription_lambdas.subscriptions_lambda)
        )

from aws_cdk import aws_apigateway as apigw
from constructs import Construct
from ..config import PROJECT_PREFIX

class ApiGateway(Construct):
    def __init__(self, scope: Construct, id: str, auth_lambdas, artist_lambdas):
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


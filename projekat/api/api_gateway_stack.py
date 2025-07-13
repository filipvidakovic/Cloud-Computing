from aws_cdk import aws_apigateway as apigw
from constructs import Construct
from ..config import PROJECT_PREFIX

class ApiGateway(Construct):
    def __init__(self, scope: Construct, id: str, lambdas):
        super().__init__(scope, id)

        api = apigw.RestApi(self, f"{PROJECT_PREFIX}AuthApi", rest_api_name=f"{PROJECT_PREFIX}CognitoAuthApi")

        register = api.root.add_resource("register")
        register.add_method("POST", apigw.LambdaIntegration(lambdas.register_lambda))

        login = api.root.add_resource("login")
        login.add_method("POST", apigw.LambdaIntegration(lambdas.login_lambda))
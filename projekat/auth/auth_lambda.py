from aws_cdk import aws_lambda as _lambda
from constructs import Construct
from ..config import PROJECT_PREFIX
from aws_cdk import aws_iam as iam
from aws_cdk import Duration

class AuthLambdas(Construct):
    def __init__(self, scope: Construct, id: str, user_pool, user_pool_client):
        super().__init__(scope, id)

        env_vars = {
            "USER_POOL_ID": user_pool.user_pool_id,
            "CLIENT_ID": user_pool_client.user_pool_client_id
        }

        self.register_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}RegisterLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="register.handler",
            code=_lambda.Code.from_asset("lambda/auth"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )

        self.register_lambda.add_to_role_policy(
           iam.PolicyStatement(
            actions=[
                "cognito-idp:SignUp", 
                "cognito-idp:AdminInitiateAuth", 
                "cognito-idp:InitiateAuth", 
                "cognito-idp:AdminConfirmSignUp"
                ],
            resources=["*"]
           )
        )

        self.login_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}LoginLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="login.handler",
            code=_lambda.Code.from_asset("lambda/auth"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )

        self.login_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cognito-idp:SignIn", 
                         "cognito-idp:AdminInitiateAuth", 
                         "cognito-idp:InitiateAuth", 
                         "cognito-idp:InitiateAuth", 
                         "cognito-idp:AdminConfirmSignUp"
                         ],
                resources=["*"]
            )
        )

        self.get_user_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetUserLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_user.handler",
            code=_lambda.Code.from_asset("lambda/auth"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )

        self.get_user_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cognito-idp:AdminGetUser"],
                resources=["*"]
            )
        )

        user_pool.grant(self.register_lambda, "cognito-idp:SignUp", "cognito-idp:AdminInitiateAuth")
        user_pool.grant(self.login_lambda, "cognito-idp:SignIn", "cognito-idp:AdminInitiateAuth")
        user_pool.grant(self.get_user_lambda, "cognito-idp:AdminGetUser")
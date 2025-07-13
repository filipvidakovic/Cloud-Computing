from aws_cdk import aws_lambda as _lambda
from constructs import Construct
from ..config import PROJECT_PREFIX

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
            code=_lambda.Code.from_asset("lambda"),
            environment=env_vars
        )

        self.login_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}LoginLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="login.handler",
            code=_lambda.Code.from_asset("lambda"),
            environment=env_vars
        )
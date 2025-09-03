from aws_cdk import aws_cognito as cognito, RemovalPolicy, CfnOutput
from constructs import Construct
from aws_cdk import aws_apigateway as apigw
from ..config import PROJECT_PREFIX

class CognitoAuth(Construct):
    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)

        self.user_pool = cognito.UserPool(
            self, f"{PROJECT_PREFIX}UserPool",
            self_sign_up_enabled=True,
            sign_in_aliases=cognito.SignInAliases(email=True, username=True),
            auto_verify=cognito.AutoVerifiedAttrs(email=False),
            standard_attributes=cognito.StandardAttributes(
                given_name=cognito.StandardAttribute(required=True, mutable=True),
                family_name=cognito.StandardAttribute(required=True, mutable=True),
                birthdate=cognito.StandardAttribute(required=True, mutable=True),
                email=cognito.StandardAttribute(required=True, mutable=False)
            ),
            password_policy=cognito.PasswordPolicy(
                min_length=8,
                require_lowercase=True,
                require_digits=True
            ),
            removal_policy=RemovalPolicy.DESTROY
        )

        self.user_pool_client = cognito.UserPoolClient(
            self, f"{PROJECT_PREFIX}UserPoolClient",
            user_pool=self.user_pool,
            generate_secret=False,
            auth_flows=cognito.AuthFlow(
                user_password=True,
                user_srp=True
            )
        )

        CfnOutput(self, "UserPoolId", value=self.user_pool.user_pool_id)
        CfnOutput(self, "UserPoolClientId", value=self.user_pool_client.user_pool_client_id)

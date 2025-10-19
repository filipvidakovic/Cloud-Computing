from aws_cdk import (
    aws_cognito as cognito,
    RemovalPolicy,
    CfnOutput
)
from constructs import Construct
from ..config import PROJECT_PREFIX


class CognitoAuth(Construct):
    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)

        # --- Create User Pool ---
        self.user_pool = cognito.UserPool(
            self, f"{PROJECT_PREFIX}UserPool",
            self_sign_up_enabled=True,
            sign_in_aliases=cognito.SignInAliases(email=True, username=True),
            auto_verify=cognito.AutoVerifiedAttrs(email=True),
            standard_attributes=cognito.StandardAttributes(
                given_name=cognito.StandardAttribute(required=True, mutable=True),
                family_name=cognito.StandardAttribute(required=True, mutable=True),
                birthdate=cognito.StandardAttribute(required=True, mutable=True),
                email=cognito.StandardAttribute(required=True, mutable=False)
            ),
            custom_attributes={
                "role": cognito.StringAttribute(mutable=True)
            },
            password_policy=cognito.PasswordPolicy(
                min_length=8,
                require_lowercase=True,
                require_digits=True
            ),
            removal_policy=RemovalPolicy.DESTROY,

            user_verification=cognito.UserVerificationConfig(
                email_subject="Verify your account",
                email_body="Welcome to our app! Click the link below to verify your email:\n{##Verify Email##}",
                email_style=cognito.VerificationEmailStyle.LINK
            )
        )

        # --- Add a Domain (Required for LINK verification) ---
        # Must be globally unique, so we add "-auth" and lower() to be safe.
        self.user_pool_domain = self.user_pool.add_domain(
            f"{PROJECT_PREFIX}Domain",
            cognito_domain=cognito.CognitoDomainOptions(
                domain_prefix=f"{PROJECT_PREFIX.lower()}-auth"
            )
        )

        # --- Create App Client ---
        self.user_pool_client = cognito.UserPoolClient(
            self, f"{PROJECT_PREFIX}UserPoolClient",
            user_pool=self.user_pool,
            generate_secret=False,
            auth_flows=cognito.AuthFlow(
                user_password=True,
                user_srp=True
            ),
            o_auth=cognito.OAuthSettings(
                # You can update these later to real URLs
                callback_urls=[
                    "http://localhost:4200",   # Local dev
                    "https://example.com"       # placeholder for future deployment
                ],
                logout_urls=[
                    "http://localhost:4200",
                    "https://example.com"
                ]
            )
        )

        # --- Outputs ---
        CfnOutput(self, "UserPoolId", value=self.user_pool.user_pool_id)
        CfnOutput(self, "UserPoolClientId", value=self.user_pool_client.user_pool_client_id)
        CfnOutput(self, "UserPoolDomain", value=self.user_pool_domain.domain_name)

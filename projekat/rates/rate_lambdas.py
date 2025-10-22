from aws_cdk import aws_lambda as _lambda
from aws_cdk import Duration
from constructs import Construct

from projekat.config import PROJECT_PREFIX

class RateLambdas(Construct):
    def __init__(self, scope: Construct, id: str, rates_table):
        super().__init__(scope, id)

        env_vars = {
            "RATES_TABLE": rates_table.table_name,
        }

        # Create / update rate
        self.create_rate_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}CreateRateLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="create_rate.lambda_handler",
            code=_lambda.Code.from_asset("lambda/rates"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )

        # Get all rates for a user
        self.get_rate_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetRateLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_rate.lambda_handler",
            code=_lambda.Code.from_asset("lambda/rates"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )

        # Delete a rate
        self.delete_rate_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}DeleteRateLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="delete_rate.lambda_handler",
            code=_lambda.Code.from_asset("lambda/rates"),
            environment=env_vars,
            timeout=Duration.seconds(10)
        )

        # Permissions
        rates_table.grant_read_write_data(self.create_rate_lambda)
        rates_table.grant_read_data(self.get_rate_lambda)
        rates_table.grant_read_write_data(self.delete_rate_lambda)
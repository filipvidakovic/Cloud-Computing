from aws_cdk import Duration, aws_lambda as _lambda
from constructs import Construct
from projekat.config import PROJECT_PREFIX

class UserLambdas(Construct):
    def __init__(self, scope: Construct, id: str, user_history_table):
        super().__init__(scope, id)

        self.record_play_lambda = _lambda.Function(
            self, f"{PROJECT_PREFIX}RecordPlayLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="record_play.lambda_handler",
            code=_lambda.Code.from_asset("lambda/user"),
            environment={
                "USER_HISTORY_TABLE": user_history_table.table_name
            },
            timeout=Duration.seconds(10)
        )

        user_history_table.grant_read_write_data(self.record_play_lambda)

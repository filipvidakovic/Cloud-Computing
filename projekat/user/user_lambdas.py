from aws_cdk import Duration, aws_lambda as _lambda
from constructs import Construct
from projekat.config import PROJECT_PREFIX


class UserLambdas(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        user_history_table,
        user_feed_table,
        user_subscriptions_table,
        user_reactions_table,
        music_table,
        song_table,
        artist_info_table,
    ):
        super().__init__(scope, id)

        # 1. Record play Lambda
        self.record_play_lambda = _lambda.Function(
            self,
            f"{PROJECT_PREFIX}RecordPlayLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="record_play.lambda_handler",
            code=_lambda.Code.from_asset("lambda/user"),
            environment={
                "USER_HISTORY_TABLE": user_history_table.table_name,
            },
            timeout=Duration.seconds(10),
        )
        user_history_table.grant_read_write_data(self.record_play_lambda)

        # 2. Feed recompute Lambda (delete old + write new recommendations)
        self.feed_recompute_lambda = _lambda.Function(
            self,
            f"{PROJECT_PREFIX}FeedRecomputeLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="feed.lambda_handler",
            code=_lambda.Code.from_asset("lambda/user"),
            environment={
                "USER_FEED_TABLE": user_feed_table.table_name,
                "USER_HISTORY_TABLE": user_history_table.table_name,
                "USER_SUBSCRIPTIONS_TABLE": user_subscriptions_table.table_name,
                "USER_REACTIONS_TABLE": user_reactions_table.table_name,
                "MUSIC_TABLE": music_table.table_name,
                "SONG_TABLE": song_table.table_name,
                "ARTIST_INFO_TABLE": artist_info_table.table_name,
            },
            timeout=Duration.seconds(60),
        )
        user_feed_table.grant_read_write_data(self.feed_recompute_lambda)
        user_history_table.grant_read_data(self.feed_recompute_lambda)
        user_subscriptions_table.grant_read_data(self.feed_recompute_lambda)
        user_reactions_table.grant_read_data(self.feed_recompute_lambda)
        music_table.grant_read_data(self.feed_recompute_lambda)
        song_table.grant_read_data(self.feed_recompute_lambda)
        artist_info_table.grant_read_data(self.feed_recompute_lambda)

        # 3. Get feed Lambda (for frontend GET /feed)
        self.get_feed_lambda = _lambda.Function(
            self,
            f"{PROJECT_PREFIX}GetFeedLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_feed.lambda_handler",
            code=_lambda.Code.from_asset("lambda/user"),
            environment={
                "USER_FEED_TABLE": user_feed_table.table_name,
                "SONG_TABLE": song_table.table_name,
            },
            timeout=Duration.seconds(10),
        )
        user_feed_table.grant_read_data(self.get_feed_lambda)
        song_table.grant_read_data(self.get_feed_lambda)
from aws_cdk import (
    Duration, Stack,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_events,
)
from constructs import Construct

class FeedQueueStack(Construct):
    def __init__(self, scope: Construct, id: str, *, env_vars: dict, producer_fns: list[_lambda.Function],
                 user_feed_table=None, user_history_table=None, user_subscriptions_table=None,
                 user_reactions_table=None, music_table=None, song_table=None, artist_info_table=None) -> None:
        super().__init__(scope, id)

        # DLQ for failures
        dlq = sqs.Queue(
            self, "UserFeedRecomputeDLQ",
            queue_name="UserFeedRecomputeDLQ.fifo",
            fifo=True,
            content_based_deduplication=True,
            retention_period=Duration.days(14),
        )
        # sqs queue for feed recomputing
        self.queue = sqs.Queue(
            self, "UserFeedRecomputeQueue",
            queue_name="UserFeedRecomputeQueue.fifo",
            fifo=True,
            content_based_deduplication=True,
            visibility_timeout=Duration.seconds(90),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=5,
                queue=dlq
            ),
        )

        # worker Lambda for wrapping feed calculation lambda
        self.worker = _lambda.Function(
            self, "FeedWorker",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="feed.lambda_sqs_handler",
            code=_lambda.Code.from_asset("lambda/user"),
            timeout=Duration.seconds(60),
            environment=env_vars | {
                "RECOMPUTE_SOURCE": "sqs",
            },
        )

        # set sqs queue as event source
        self.worker.add_event_source(lambda_events.SqsEventSource(
            self.queue,
            batch_size=10,
        ))

        if user_feed_table:
            user_feed_table.grant_read_write_data(self.worker)
        if user_history_table:
            user_history_table.grant_read_data(self.worker)
        if user_subscriptions_table:
            user_subscriptions_table.grant_read_data(self.worker)
        if user_reactions_table:
            user_reactions_table.grant_read_data(self.worker)
        if music_table:
            music_table.grant_read_data(self.worker)
        if song_table:
            song_table.grant_read_data(self.worker)
        if artist_info_table:
            artist_info_table.grant_read_data(self.worker)

        # set producers for this queue
        for fn in producer_fns:
            self.queue.grant_send_messages(fn)
            fn.add_environment("RECOMPUTE_QUEUE_URL", self.queue.queue_url)

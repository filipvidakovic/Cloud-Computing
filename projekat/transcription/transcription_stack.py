import aws_cdk as cdk
from aws_cdk import (
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as s3n
)
from constructs import Construct
from ..config import PROJECT_PREFIX


class TranscriptionStack(Construct):
    def __init__(self, scope, id, song_bucket, song_table, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Lambda 1: starts transcription job when song uploaded
        self.start_fn = _lambda.Function(
            self, f"{PROJECT_PREFIX}StartTranscriptionLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="start_transcription.handler",
            code=_lambda.Code.from_asset("lambda/transcription"),
            environment={
                "SONG_TABLE": song_table.table_name,
                "SONG_BUCKET": song_bucket.bucket_name,
            },
            timeout=cdk.Duration.minutes(15),
            memory_size=1024,
        )

        song_bucket.grant_read(self.start_fn)
        song_bucket.grant_write(self.start_fn)
        song_table.grant_read_write_data(self.start_fn)

        self.start_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["transcribe:StartTranscriptionJob"],
            resources=["*"],
        ))

        # Trigger Lambda 1 when new song is uploaded
        song_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.start_fn),
            s3.NotificationKeyFilter(prefix="music/")
        )

        # Lambda 2: processes transcription JSON
        self.process_fn = _lambda.Function(
            self, f"{PROJECT_PREFIX}ProcessTranscriptionLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="process_transcription.handler",
            code=_lambda.Code.from_asset("lambda/transcription"),
            environment={
                "SONG_TABLE": song_table.table_name,
                "SONG_BUCKET": song_bucket.bucket_name,
            },
            timeout=cdk.Duration.minutes(5),
            memory_size=512,
        )

    
        song_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.process_fn),
            s3.NotificationKeyFilter(prefix="transcriptions/", suffix=".json")
        )

        song_table.grant_write_data(self.process_fn)
        song_bucket.grant_read(self.process_fn)

        # Lambda 3: fetch transcription via API
        self.get_fn = _lambda.Function(
            self, f"{PROJECT_PREFIX}GetTranscriptionLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="get_transcription.handler",
            code=_lambda.Code.from_asset("lambda/transcription"),
            environment={
                "SONG_TABLE": song_table.table_name,
                "SONG_BUCKET": song_bucket.bucket_name,
            },
            timeout=cdk.Duration.seconds(30),
            memory_size=256,
        )

        song_table.grant_read_data(self.get_fn)

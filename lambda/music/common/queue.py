import os, json, time
import boto3

sqs = boto3.client("sqs")
QUEUE_URL = os.environ["RECOMPUTE_QUEUE_URL"]

def enqueue_recompute(user_id: str, reason: str, music_id: str | None = None):
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps({
            "userId": user_id,
            "reason": reason,   # "subscribe" | "unsubscribe" | "rate" | "play"
            "musicId": music_id,
            "ts": int(time.time())
        }),
        MessageGroupId=user_id,
        MessageDeduplicationId=f"{user_id}-{reason}-{int(time.time())//10}"
    )
    print(f"📨 Sent message to SQS")


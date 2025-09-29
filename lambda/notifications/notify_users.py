import os
import json
import boto3
import uuid
from datetime import datetime
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
subscriptions_table = dynamodb.Table(os.environ["SUBSCRIPTIONS_TABLE"])
notifications_table = dynamodb.Table(os.environ["NOTIFICATIONS_TABLE"])


def handler(event, context):
    """
    SNS → Lambda trigger.
    Event contains a message about a new upload (song/album/etc.).
    Writes notifications for all subscribed users.
    """
    print("Received event:", json.dumps(event))

    # Extract SNS message
    for record in event.get("Records", []):
        sns_msg = json.loads(record["Sns"]["Message"])
        target_id = sns_msg["targetId"]   # e.g. artistId, albumId
        subscription_type = sns_msg["subscriptionType"]  # e.g. "artist"

        # Find users subscribed to this target
        response = subscriptions_table.query(
            IndexName="SubscriptionTypeTargetIdIndex",
            KeyConditionExpression=Key("subscriptionType").eq(subscription_type) &
                                   Key("targetId").eq(target_id)
        )

        subscribers = response.get("Items", [])
        print(f"Found {len(subscribers)} subscribers")

        # Create a notification for each user
        for sub in subscribers:
            user_id = sub["userId"]

            notification_item = {
                "userId": user_id,
                "notificationId": str(uuid.uuid4()),
                "type": subscription_type,
                "targetId": target_id,
                "title": sns_msg.get("title", "New update"),
                "timestamp": datetime.utcnow().isoformat(),
                "read": "false"
            }

            notifications_table.put_item(Item=notification_item)

    return {"statusCode": 200, "body": "Notifications created"}

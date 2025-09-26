import decimal
import os
import json
import time
import boto3

dynamodb = boto3.resource("dynamodb")
history_table = dynamodb.Table(os.environ["USER_HISTORY_TABLE"])

MAX_HISTORY = 40
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)

def response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST"
        },
        "body": json.dumps(body, cls=DecimalEncoder)
    }

def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")

        claims = event["requestContext"]["authorizer"]["claims"]
        user_id = claims["sub"]

        genre = body.get("genre")

        if not genre:
            return response(400, {"error": "genre is required"})

        play_entry = {
            "genre": genre,
            "playedAt": int(time.time())
        }

        # Append new play atomically
        history_table.update_item(
            Key={"userId": user_id},
            UpdateExpression="SET recentPlays = list_append(if_not_exists(recentPlays, :empty), :new)",
            ExpressionAttributeValues={
                ":new": [play_entry],
                ":empty": []
            }
        )

        # Trim if longer than MAX_HISTORY
        item = history_table.get_item(Key={"userId": user_id}).get("Item", {})
        history = item.get("recentPlays", [])
        if len(history) > MAX_HISTORY:
            history = history[-MAX_HISTORY:]
            history_table.put_item(Item={"userId": user_id, "recentPlays": history})

        return response(200, {"message": "Play recorded", "history": history})

    except Exception as e:
        return response(500, {"error": str(e)})

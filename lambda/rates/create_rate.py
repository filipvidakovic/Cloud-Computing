import os, json, boto3
from datetime import datetime
from common.queue import enqueue_recompute


dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["RATES_TABLE"])

def get_user_id(event):
    rc = event.get("requestContext", {})
    auth = rc.get("authorizer", {})
    if "claims" in auth:   
        return auth["claims"].get("sub")
    return None

def build_response(status, body=""):
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",  # for dev; restrict in prod
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET,DELETE",
        },
        "body": json.dumps(body) if body else ""
    }

def lambda_handler(event, context):
    # Handle preflight CORS
    if event.get("httpMethod") == "OPTIONS":
        return build_response(200)

    body = json.loads(event.get("body", "{}"))
    user_id = get_user_id(event)
    music_id = body.get("musicId")
    rate = body.get("rate")  # "love" | "like" | "dislike"

    if not user_id or not music_id or rate not in ["love", "like", "dislike"]:
        return build_response(400, {"error": "Invalid input"})

    now = datetime.utcnow().isoformat()
    table.put_item(Item={
        "userId": user_id,
        "musicId": music_id,
        "rate": rate,
        "createdAt": now,
        "updatedAt": now
    })

    # Send SQS message to recompute feed
    enqueue_recompute(user_id, "rate", music_id)

    return build_response(201, {"message": "Rate saved"})

import os, json, boto3, uuid
from datetime import datetime

def get_user_id(event):
    rc = event.get("requestContext", {})
    auth = rc.get("authorizer", {})
    if "claims" in auth:   
        return auth["claims"].get("sub")
    return None

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["RATES_TABLE"])

def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))
    user_id = get_user_id(event)
    music_id = body.get("musicId")
    rate = body.get("rate")  # "love" | "like" | "dislike"
    print(event)
    print(f"USER_ID: {user_id}, MUSIC_ID: {music_id}, RATE: {rate}")

    if not user_id or not music_id or rate not in ["love", "like", "dislike"]:
        return {"statusCode": 400, "body": json.dumps({"error": "Invalid input"})}

    now = datetime.utcnow().isoformat()
    table.put_item(Item={
        "userId": user_id,
        "musicId": music_id,
        "rate": rate,
        "createdAt": now,
        "updatedAt": now
    })

    return {"statusCode": 201, "body": json.dumps({"message": "Rate saved"})}

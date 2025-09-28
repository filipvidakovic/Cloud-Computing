import os, json, boto3

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["RATES_TABLE"])

def get_user_id(event):
    rc = event.get("requestContext", {})
    auth = rc.get("authorizer", {})
    if "claims" in auth:   
        return auth["claims"].get("sub")
    return None

def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))
    user_id = get_user_id(event)
    music_id = body.get("musicId")

    if not user_id or not music_id:
        return {"statusCode": 400, "body": json.dumps({"error": "userId and musicId are required"})}

    table.delete_item(Key={"userId": user_id, "musicId": music_id})

    return {"statusCode": 200, "body": json.dumps({"message": "Rate deleted"})}

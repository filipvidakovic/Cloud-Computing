import os, json, boto3

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
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET,DELETE",
        },
        "body": json.dumps(body) if body else ""
    }

def lambda_handler(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return build_response(200)

    body = json.loads(event.get("body", "{}"))
    user_id = get_user_id(event)
    music_id = body.get("musicId")

    if not user_id or not music_id:
        return build_response(400, {"error": "userId and musicId are required"})

    table.delete_item(Key={"userId": user_id, "musicId": music_id})

    return build_response(200, {"message": "Rate deleted"})

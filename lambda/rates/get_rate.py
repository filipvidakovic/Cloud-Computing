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
    user_id = get_user_id(event)
    if not user_id:
        return {"statusCode": 400, "body": json.dumps({"error": "userId is required"})}

    resp = table.query(
        KeyConditionExpression="userId = :u",
        ExpressionAttributeValues={":u": user_id}
    )

    return {"statusCode": 200, "body": json.dumps(resp.get("Items", []))}

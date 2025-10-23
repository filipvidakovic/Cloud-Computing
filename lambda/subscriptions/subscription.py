import os
import json
import boto3
from datetime import datetime
from boto3.dynamodb.conditions import Key
from common.queue import enqueue_recompute

def get_user_id(event):
    rc = event.get("requestContext", {})
    auth = rc.get("authorizer", {})
    if "claims" in auth:   
        return auth["claims"].get("sub")
    return None

# --- AWS Clients ---
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
cognito = boto3.client("cognito-idp")

TABLE_NAME = os.environ["SUBSCRIPTIONS_TABLE"]
NOTIFICATIONS_TOPIC_ARN = os.environ["NOTIFICATIONS_TOPIC_ARN"]
USER_POOL_ID = os.environ["USER_POOL_ID"]

table = dynamodb.Table(TABLE_NAME)


# --- Helpers ---
def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,DELETE,GET"
        },
        "body": json.dumps(body)
    }


def cors_response():
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,DELETE,GET"
        },
        "body": json.dumps({"message": "CORS preflight request"})
    }


def get_user_sub(event):
    claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
    return claims.get("sub")


def get_user_email(sub: str):
    """Get Cognito user email from sub claim."""
    resp = cognito.list_users(
        UserPoolId=USER_POOL_ID,
        Filter=f'sub = "{sub}"'
    )
    users = resp.get("Users", [])
    if not users:
        return None
    attrs = {a["Name"]: a["Value"] for a in users[0].get("Attributes", [])}
    return attrs.get("email")


# --- Handlers ---
def handle_post(event):
    user_sub = get_user_sub(event)
    if not user_sub:
        return response(401, {"error": "Unauthorized"})

    body = json.loads(event.get("body", "{}"))
    subscription_type = body.get("type")
    target_id = body.get("id")

    if not subscription_type or not target_id:
        return response(400, {"error": "type and id are required"})

    # Get user's email from Cognito automatically
    email = get_user_email(user_sub)
    if not email:
        return response(500, {"error": "Could not retrieve user email from Cognito"})

    subscription_key = f"{subscription_type}#{target_id}"

    item = {
        "userId": user_sub,
        "subscriptionId": subscription_key,
        "subscriptionType": subscription_type,
        "targetId": target_id,
        "email": email,
        "createdAt": datetime.utcnow().isoformat()
    }

    # Save subscription
    table.put_item(Item=item)

    # Subscribe to SNS topic
    try:
        user_id = get_user_id(event)
        if not user_id:
            return response(401, {"error": "Unauthorized"})

        body = json.loads(event.get("body", "{}"))
        subscription_type = body.get("type")
        target_id = body.get("id")

        if not user_id or not subscription_type or not target_id:
            return response(400, {"error": "userId, type, and id are required"})

        subscription_key = f"{subscription_type}#{target_id}"

        item = {
            "userId": user_id,
            "subscriptionId": subscription_key,
            "subscriptionType": subscription_type,
            "targetId": target_id,
            "createdAt": datetime.utcnow().isoformat()
        }

        table.put_item(Item=item)

        # Send SQS message to recompute feed
        enqueue_recompute(user_id, "subscribe", target_id)

        sns.subscribe(
            TopicArn=NOTIFICATIONS_TOPIC_ARN,
            Protocol="email",
            Endpoint=email
        )
    except Exception as e:
        print(f"Failed to subscribe {email} to SNS: {e}")

    return response(200, {"message": f"Subscribed {email} to {subscription_type} {target_id}", "item": item})


def handle_get(event):
    user_sub = get_user_sub(event)
    if not user_sub:
        return response(401, {"error": "Unauthorized"})

    result = table.query(KeyConditionExpression=Key("userId").eq(user_sub))
    items = result.get("Items", [])
    artist_subs = [i for i in items if i["subscriptionType"] == "artist"]
    genre_subs = [i for i in items if i["subscriptionType"] == "genre"]

    return response(200, {"artistSubscriptions": artist_subs, "genreSubscriptions": genre_subs})


def handle_delete(event):
    user_sub = get_user_sub(event)
    if not user_sub:
        return response(401, {"error": "Unauthorized"})

    path_params = event.get("pathParameters") or {}
    key = path_params.get("subscriptionKey")

    try:
        subscription_type, target_id = key.split("=", 1)
        subscription_key = f"{subscription_type}#{target_id}"
        if not subscription_key or "#" not in subscription_key:
            return response(400, {"error": "subscriptionKey is required and must be type#id"})
    except Exception:
        return response(400, {"error": "Invalid subscriptionKey format"})
    
    try:
        table.delete_item(
            Key={
                "userId": user_sub,
                "subscriptionId": subscription_key
            },
            ConditionExpression="attribute_exists(subscriptionId)"
        )
        # Send SQS message to recompute feed
        enqueue_recompute(user_sub, "unsubscribe", subscription_key)

    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return response(404, {"error": "Subscription not found"})
    except Exception as e:
        return response(500, {"error": str(e)})

    return response(200, {"message": "Unsubscribed successfully"})


# --- Main Lambda Handler ---
def handler(event, context):
    method = event.get("httpMethod", "")

    if method == "OPTIONS":
        return cors_response()
    elif method == "POST":
        return handle_post(event)
    elif method == "GET":
        return handle_get(event)
    elif method == "DELETE":
        return handle_delete(event)
    else:
        return response(405, {"error": "Method Not Allowed"})

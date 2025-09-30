import json
import boto3
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
sns = boto3.client("sns")

# DynamoDB table from environment variable
table = dynamodb.Table(os.environ['SUBSCRIPTIONS_TABLE'])
# SNS topic ARN from environment variable
NOTIFICATIONS_TOPIC_ARN = os.environ["NOTIFICATIONS_TOPIC_ARN"]


def get_user_id(event):
    rc = event.get("requestContext", {})
    auth = rc.get("authorizer", {})
    if "claims" in auth:   
        return auth["claims"].get("sub")
    return None


def handler(event, context):
    method = event.get("httpMethod", "")

    if method == "OPTIONS":
        return cors_response()

    if method == "POST":
        return handle_post(event)
    elif method == "GET":
        return handle_get(event)
    elif method == "DELETE":
        return handle_delete(event)
    else:
        return response(405, {"error": "Method Not Allowed"})


def handle_post(event):
    try:
        user_id = get_user_id(event)
        if not user_id:
            return response(401, {"error": "Unauthorized"})

        body = json.loads(event.get("body", "{}"))
        subscription_type = body.get("type")
        target_id = body.get("id")
        email = body.get("email")   # ✅ client must send email

        if not user_id or not subscription_type or not target_id or not email:
            return response(400, {"error": "userId, type, id, and email are required"})

        # Save subscription to DynamoDB
        subscription_key = f"{subscription_type}#{target_id}"
        item = {
            "userId": user_id,
            "subscriptionId": subscription_key,
            "subscriptionType": subscription_type,
            "targetId": target_id,
            "email": email,
            "createdAt": datetime.utcnow().isoformat()
        }
        table.put_item(Item=item)

        # Subscribe email to SNS topic
        sns.subscribe(
            TopicArn=NOTIFICATIONS_TOPIC_ARN,
            Protocol="email",
            Endpoint=email
        )

        return response(
            200,
            {
                "message": f"Subscribed {email} to {subscription_type} {target_id}",
                "item": item
            }
        )

    except Exception as e:
        return response(500, {"error": str(e)})


def handle_get(event):
    try:
        user_id = get_user_id(event)
        if not user_id:
            return response(401, {"error": "Unauthorized"})

        result = table.query(
            KeyConditionExpression=Key("userId").eq(user_id)
        )
        items = result.get('Items', [])
        artist_subs = [item for item in items if item['subscriptionType'] == 'artist']
        genre_subs = [item for item in items if item['subscriptionType'] == 'genre']
        return response(200, {"artistSubscriptions": artist_subs, "genreSubscriptions": genre_subs})
    except Exception as e:
        return response(500, {"error": str(e)})


def handle_delete(event):
    path_params = event.get("pathParameters") or {}
    key = path_params.get("subscriptionKey")
    subscription_type, subscription_id = key.split("=") if key else None
    subscription_key = f"{subscription_type}#{subscription_id}" if subscription_type and subscription_id else None

    if not subscription_key:
        return response(400, {"error": "subscriptionKey is required"})

    claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
    user_id = claims.get("sub")
    if not user_id:
        return response(401, {"error": "Unauthorized"})

    try:
        table.delete_item(
            Key={
                "userId": user_id,
                "subscriptionId": subscription_key
            },
            ConditionExpression="attribute_exists(subscriptionId)"
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return response(404, {"error": "Subscription not found"})
    except Exception as e:
        return response(500, {"error": str(e)})

    return response(200, {"message": "Unsubscribed successfully"})


def response(status, body):
    """Standard JSON response with CORS."""
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,DELETE,GET"
        },
        "body": json.dumps(body)
    }


def cors_response():
    """Handle OPTIONS preflight requests."""
    return {
        "body": json.dumps({"message": "CORS preflight request"}),
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,DELETE,GET"
        }
    }

import json
import boto3
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key

from ..get_user_id import get_user_id


# Initialize DynamoDB table from environment variable
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['SUBSCRIPTIONS_TABLE'])

def lambda_handler(event, context):
    """Main Lambda entry point."""
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
        # body = json.loads(event.get('body', '{}'))
        
        user_id = get_user_id(event)
        # subscription_key = event["pathParameters"]["subscriptionKey"]
        if not user_id:
            return response(403, {"error": "Unauthorized"})
        return response(200, {"message": f"Hello, user {user_id}"})
        subscription_type = body.get('type')
        target_id = body.get('id')  
        action = body.get('action', 'subscribe')

        if not user_id or not subscription_type or not target_id:
            return response(400, {"error": "userId, type, and id are required"})

        subscription_key = f"{subscription_type}#{target_id}"

        if action == "subscribe":
            item = {
                "userId": user_id,
                "subscriptionId": subscription_key,
                "subscriptionType": subscription_type,
                "targetId": target_id,
                "createdAt": datetime.utcnow().isoformat()
            }
            table.put_item(Item=item)
            return response(200, {"message": f"Subscribed to {subscription_type} {target_id}", "item": item})

        elif action == "unsubscribe":
            table.delete_item(Key={"userId": user_id, "subscriptionId": subscription_key})
            return response(200, {"message": f"Unsubscribed from {subscription_type} {target_id}"})

        else:
            return response(400, {"error": "Invalid action"})
    except Exception as e:
        return response(500, {"error": str(e)})

def handle_get(event):
    try:
        rc = event.get("requestContext", {})
        print(rc)
        auth = rc.get("authorizer", {})
        print(auth)
        user_id = ""
        if "claims" in auth:
            user_id = auth["claims"].get("sub")
        # user_id = get_user_id(event)
        print(user_id)
        if not user_id:
            return response(400, {"error": "userId is required"})

        result = table.query(
            KeyConditionExpression=Key("userId").eq(user_id)
        )
        return response(200, {"subscriptions": result.get('Items', [])})
    except Exception as e:
        return response(500, {"error": str(e)})

def handle_delete(event):
    path_params = event.get("pathParameters") or {}
    artist_id = path_params.get("artistId")
    if not artist_id:
        return response(400, {"error": "artistId is required"})

    claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
    user_id = claims.get("sub")
    if not user_id:
        return response(403, {"error": "Unauthorized"})

    subscription_id = f"artist#{artist_id}"

    try:
        table.delete_item(
            Key={
                "userId": user_id,
                "subscriptionId": subscription_id
            },
            ConditionExpression="attribute_exists(subscriptionId)"
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return response(404, {"error": "Subscription not found"})

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
        "body": {"message": "CORS preflight request"},
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,DELETE,GET"
        }
    }

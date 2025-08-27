import json
import boto3
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key

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
        body = json.loads(event.get('body', '{}'))
        user_id = body.get('userId')   # e.g. userId
        subscription_type = body.get('type')  # e.g. "artist" or "album"
        target_id = body.get('id')  # e.g. artistId or albumId
        action = body.get('action', 'subscribe')  # e.g. "subscribe" or "unsubscribe"

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
        user_id = event.get('queryStringParameters', {}).get('userId')
        if not user_id:
            return response(400, {"error": "userId is required"})

        result = table.query(
            KeyConditionExpression=Key("userId").eq(user_id)
        )
        return response(200, {"subscriptions": result.get('Items', [])})
    except Exception as e:
        return response(500, {"error": str(e)})

def handle_delete(event):
    try:
        body = json.loads(event.get('body', '{}'))
        user_id = body.get('userId')
        subscription_type = body.get('type')
        target_id = body.get('id')

        if not user_id or not subscription_type or not target_id:
            return response(400, {"error": "userId, type, and id are required"})

        subscription_key = f"{subscription_type}#{target_id}"

        table.delete_item(Key={"userId": user_id, "subscriptionId": subscription_key})
        return response(200, {"message": f"Subscription {subscription_key} deleted"})
    except Exception as e:
        return response(500, {"error": str(e)})

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

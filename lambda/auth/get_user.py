import os
import json
import boto3

client = boto3.client("cognito-idp")

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,DELETE"
        },
        "body": json.dumps(body)
    }

def handler(event, context):
    params = event.get("pathParameters", {}) or {}
    username = params.get("username")

    if not username:
        return response(400, {"error": "username is required"})

    try:
        resp = client.admin_get_user(
            UserPoolId=os.environ["USER_POOL_ID"],
            Username=username
        )

        # Extract attributes into a dictionary
        attrs = {attr["Name"]: attr["Value"] for attr in resp["UserAttributes"]}

        user = {
            "username": resp["Username"],
            "email": attrs.get("email", ""),
            "firstName": attrs.get("given_name", ""),
            "lastName": attrs.get("family_name", ""),
            "birthdate": attrs.get("birthdate", "")
        }

        return response(200, user)

    except client.exceptions.UserNotFoundException:
        return response(404, {"error": "User not found"})
    except Exception as e:
        return response(500, {"error": str(e)})

import json
import boto3
import os

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        "body": json.dumps(body)
    }

def handler(event, context):
    client = boto3.client("cognito-idp")
    body = json.loads(event.get("body", "{}"))

    try:
        required_fields = ["username", "email", "password", "first_name", "last_name", "birthdate"]
        for field in required_fields:
            if field not in body or not body[field]:
                return response(400, {"error": f"Missing field: {field}"})

        # Default every new user to role "user"
        client.sign_up(
            ClientId=os.environ["CLIENT_ID"],
            Username=body["username"],
            Password=body["password"],
            UserAttributes=[
                {"Name": "email", "Value": body["email"]},
                {"Name": "given_name", "Value": body["first_name"]},
                {"Name": "family_name", "Value": body["last_name"]},
                {"Name": "birthdate", "Value": body["birthdate"]},
                {"Name": "custom:role", "Value": "user"}
            ]
        )

        client.admin_confirm_sign_up(
            UserPoolId=os.environ["USER_POOL_ID"],
            Username=body["username"]
        )

        return response(200, {"message": "User registered successfully", "role": "user"})

    except Exception as e:
        return response(400, {"error": str(e)})

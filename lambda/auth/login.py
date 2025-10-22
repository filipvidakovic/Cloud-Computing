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
        auth_resp = client.initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                "USERNAME": body["username"],
                "PASSWORD": body["password"]
            },
            ClientId=os.environ["CLIENT_ID"]
        )

        # Get tokens
        tokens = auth_resp["AuthenticationResult"]

        # Fetch role
        user_resp = client.admin_get_user(
            UserPoolId=os.environ["USER_POOL_ID"],
            Username=body["username"]
        )

        role = None
        for attr in user_resp["UserAttributes"]:
            if attr["Name"] == "custom:role":
                role = attr["Value"]
                break

        return response(200, {
            "access_token": tokens["AccessToken"],
            "refresh_token": tokens.get("RefreshToken"),
            "id_token": tokens["IdToken"],
            "role": role
        })

    except Exception as e:
        return response(400, {"error": str(e)})

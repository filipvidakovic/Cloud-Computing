import json

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
    import boto3, json, os

    client = boto3.client("cognito-idp")
    body = json.loads(event.get("body", "{}"))

    try:
        resp = client.initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                "USERNAME": body["username"],
                "PASSWORD": body["password"]
            },
            ClientId=os.environ["CLIENT_ID"]
        )
        return response(200, resp["AuthenticationResult"])
    except Exception as e:
        return response(400, {"error": str(e)})
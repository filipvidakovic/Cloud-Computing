def handler(event, context):
    import boto3, json, os

    client = boto3.client("cognito-idp")
    body = json.loads(event.get("body", "{}"))

    try:
        resp = client.initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                "USERNAME": body["email"],
                "PASSWORD": body["password"]
            },
            ClientId=os.environ["CLIENT_ID"]
        )
        return {"statusCode": 200, "body": json.dumps(resp["AuthenticationResult"])}
    except Exception as e:
        return {"statusCode": 400, "body": json.dumps({"error": str(e)})}
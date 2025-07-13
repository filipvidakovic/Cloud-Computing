def handler(event, context):
    import boto3, json, os

    client = boto3.client("cognito-idp")
    body = json.loads(event.get("body", "{}"))

    try:
        client.sign_up(
            ClientId=os.environ["CLIENT_ID"],
            Username=body["email"],
            Password=body["password"],
            UserAttributes=[{"Name": "email", "Value": body["email"]}]
        )
        return {"statusCode": 200, "body": json.dumps({"message": "User registered"})}
    except Exception as e:
        return {"statusCode": 400, "body": json.dumps({"error": str(e)})}
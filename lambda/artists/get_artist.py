import os
import json
import boto3

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["ARTIST_TABLE"])


def lambda_handler(event, context):
    # Get artistId from query string
    artist_id = event.get("queryStringParameters", {}).get("artistId")

    if not artist_id:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing artistId"})}

    # Fetch the artist from DynamoDB
    response = table.get_item(Key={"artistId": artist_id})
    artist = response.get("Item")

    if not artist:
        return {"statusCode": 404, "body": json.dumps({"error": "Artist not found"})}

    # Return artist info
    return {
        "statusCode": 200,
        "body": json.dumps(artist)
    }

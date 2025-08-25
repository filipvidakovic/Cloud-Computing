import os
import json
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["ARTISTS_TABLE"])

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

def lambda_handler(event, context):
    params = event.get("queryStringParameters", {}) or {}
    artist_id = params.get("artistId")

    if not artist_id:
        return response(400, {"error": "artistId is required"})

    response_query = table.query(
        KeyConditionExpression=Key("artistId").eq(artist_id)
    )
    items = response_query.get("Items", [])

    if not items:
        return response(404, {"error": "Artist not found"})

    first = items[0]
    artist = {
        "artistId": artist_id,
        "name": first["name"],
        "lastname": first["lastname"],
        "age": first["age"],
        "bio": first.get("bio", ""),
        "genres": [item["genre"] for item in items]
    }

    return response(200, artist)

import os
import json
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    raise TypeError

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
        "body": json.dumps(body, default=decimal_default)
    }

def lambda_handler(event, context):
    params = event.get("queryStringParameters", {}) or {}
    genre = params.get("genre")

    if not genre:
        return response(400, {"error": "genre is required"})
    #query GSI for genre
    resp = table.query(
        IndexName="GenreIndex",
        KeyConditionExpression=Key("genre").eq(genre)
    )
    items = resp.get("Items", [])


    # group by artistId and merge genres
    artists_map = {}
    for item in items:
        aid = item["artistId"]
        if aid not in artists_map:
            artists_map[aid] = {
                "artistId": aid,
                "name": item["name"],
                "lastname": item["lastname"],
                "age": item["age"],
                "bio": item.get("bio", ""),
                "genres": []
            }
        artists_map[aid]["genres"].append(item["genre"])

    return response(200, list(artists_map.values()))

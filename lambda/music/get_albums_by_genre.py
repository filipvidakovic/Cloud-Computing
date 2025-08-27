import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    raise TypeError

# Initialize DynamoDB client and table
dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("MUSIC_TABLE", "MusicTable")
table = dynamodb.Table(TABLE_NAME)

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,GET"
        },
        "body": json.dumps(body, default=decimal_default)
    }

def lambda_handler(event, context):
    genre = event.get("queryStringParameters", {}).get("genre")
    if not genre:
        return response(400, {"error": "genre is required while filtering"})

    try:
        dynamo_response = table.query(
            KeyConditionExpression=Key("genre").eq(genre)
        )
        items = dynamo_response.get("Items", [])

        albums = {}
        for item in items:
            album_id = item.get("albumId")
            if not album_id:
                continue

            if album_id not in albums:
                albums[album_id] = {
                    "albumId": album_id,
                    "musicIds": [item.get("musicId")],
                    "titleList": [item.get("title")],
                    "coverUrl": None,
                }
            else:
                albums[album_id]["musicIds"].append(item.get("musicId"))
                albums[album_id]["titleList"].append(item.get("title"))

            if not albums[album_id]["coverUrl"] and item.get("coverUrl"):
                albums[album_id]["coverUrl"] = item["coverUrl"]

        album_list = list(albums.values())

        return response(200, {
            "albums": album_list,
            "rawItems": items,  # 🚨 TEMPORARY — remove before production!
        })

    except Exception as e:
        return response(500, {"error": str(e)})

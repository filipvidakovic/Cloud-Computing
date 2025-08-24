import json
import os
import boto3
from boto3.dynamodb.conditions import Key

# Initialize DynamoDB client and table name
dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("MUSIC_TABLE", "MusicTable")
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    genre = event.get("genre")
    if not genre:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "genre is required while filtering"})
        }

    # Query music table by genre
    response = table.query(
        KeyConditionExpression=Key("genre").eq(genre)
    )
    items = response.get("Items", [])

    # Extract unique albums for that genre
    albums = {}
    for item in items:
        album_id = item.get("albumId")
        if album_id and album_id not in albums:
            albums[album_id] = {
                "albumId": album_id,
                "genre": item.get("genre"),
                "titleList": [item.get("title")],  # list of tracks
            }
        elif album_id:
            albums[album_id]["titleList"].append(item.get("title"))

    # Convert to list for response
    album_list = list(albums.values())

    return {
        "statusCode": 200,
        "body": json.dumps(album_list)
    }

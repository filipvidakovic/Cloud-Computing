import os
import boto3
from boto3.dynamodb.conditions import Key

MUSIC_TABLE = os.environ["MUSIC_TABLE"]
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(MUSIC_TABLE)

def lambda_handler(event, context):
    """
    Returns unique artists for a given genre
    Expects query string parameter: genre
    """
    genre = event.get("queryStringParameters", {}).get("genre")
    if not genre:
        return {
            "statusCode": 400,
            "body": "Missing 'genre' query parameter"
        }

    # Query by genre (partition key)
    response = table.query(
        KeyConditionExpression=Key("genre").eq(genre)
    )

    items = response.get("Items", [])

    # Collect unique artists
    artist_dict = {}
    for item in items:
        for artist_id in item.get("artistIds", []):
            if artist_id not in artist_dict:
                artist_dict[artist_id] = {
                    "artistId": artist_id,
                    "genre": genre
                }

    return {
        "statusCode": 200,
        "body": list(artist_dict.values())
    }

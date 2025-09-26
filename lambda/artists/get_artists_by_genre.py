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
client = boto3.client("dynamodb")

artist_table = dynamodb.Table(os.environ["ARTISTS_TABLE"])
info_table_name = os.environ["ARTIST_INFO_TABLE"]

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,DELETE"
        },
        "body": json.dumps(body, default=decimal_default)
    }

def lambda_handler(event, context):
    try:
        params = event.get("queryStringParameters", {}) or {}
        genre = params.get("genre")

        if not genre:
            return response(400, {"error": "genre is required"})

        # get artist ids for given genre
        resp = artist_table.query(
            IndexName="GenreIndex",
            KeyConditionExpression=Key("genre").eq(genre),
            ProjectionExpression="artistId"
        )
        items = resp.get("Items", [])
        artist_ids = list({it["artistId"] for it in items})

        if not artist_ids:
            return response(200, {"artists": []})

        # get artist profiles from ArtistInfoTable
        def chunks(lst, n=100):
            for i in range(0, len(lst), n):
                yield lst[i:i+n]

        artists = []
        for chunk in chunks(artist_ids):
            resp = client.batch_get_item(
                RequestItems={
                    info_table_name: {
                        "Keys": [{"artistId": {"S": aid}} for aid in chunk]
                    }
                }
            )
            profiles = resp["Responses"].get(info_table_name, [])
            for p in profiles:
                artists.append({
                    "artistId": p["artistId"]["S"],
                    "name": p["name"]["S"],
                    "lastname": p["lastname"]["S"],
                    "age": int(p["age"]["N"]),
                    "bio": p.get("bio", {}).get("S", ""),
                    "genres": [g["S"] for g in p.get("genres", {}).get("L", [])]
                })

        return response(200, {"artists": artists})

    except Exception as e:
        return response(500, {"error": str(e)})

import os
import json
import boto3

dynamodb = boto3.resource("dynamodb")
info_table = dynamodb.Table(os.environ["ARTIST_INFO_TABLE"])

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,DELETE"
        },
        "body": json.dumps(body)
    }

def lambda_handler(event, context):
    try:
        params = event.get("pathParameters", {}) or {}
        artist_id = params.get("artistId")

        if not artist_id:
            return response(400, {"error": "artistId is required"})

        resp = info_table.get_item(Key={"artistId": artist_id})
        artist = resp.get("Item")

        if not artist:
            return response(404, {"error": "Artist not found"})

        if "age" in artist:
            artist["age"] = int(artist["age"])

        return response(200, artist)

    except Exception as e:
        return response(500, {"error": str(e)})

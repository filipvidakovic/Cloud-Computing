import decimal
import json
import boto3
import os
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["MUSIC_TABLE"])
client = table.meta.client
deser = TypeDeserializer()

# Custom encoder for Decimal values from DynamoDB
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        "body": json.dumps(body, cls=DecimalEncoder, ensure_ascii=False)
    }

def _marshal_keys(genre, ids):
    return [{"genre": {"S": genre}, "musicId": {"S": mid}} for mid in ids]

def lambda_handler(event, context):
    # Handle CORS preflight request
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    if event.get("httpMethod") != "POST":
        return response(405, {"error": "Method not allowed"})

    try:
        body = json.loads(event.get("body") or "{}")
        genre = body.get("genre")
        music_ids = body.get("musicIds")

        if not genre or not isinstance(genre, str):
            return response(400, {"error": "genre (string) is required"})
        if not isinstance(music_ids, list) or not music_ids:
            return response(400, {"error": "musicIds (non-empty array) is required"})

        CHUNK = 100  # DynamoDB limit
        projection = "genre, musicId, albumId, title, fileUrl, coverUrl, artistIds, fileName, fileType, fileSize, createdAt"

        found = []
        for i in range(0, len(music_ids), CHUNK):
            keys_chunk = _marshal_keys(genre, music_ids[i:i + CHUNK])

            req = {table.name: {"Keys": keys_chunk, "ProjectionExpression": projection}}
            to_get = req

            # retry loop for UnprocessedKeys
            for _ in range(5):
                res = client.batch_get_item(RequestItems=to_get)
                raw = res.get("Responses", {}).get(table.name, [])
                items = [{k: deser.deserialize(v) for k, v in it.items()} for it in raw]
                found.extend(items)
                unp = res.get("UnprocessedKeys", {})
                if not unp or not unp.get(table.name, {}).get("Keys"):
                    break
                to_get = unp

        # De-duplicate by musicId (defensive)
        seen = set()
        songs = []
        for it in found:
            mid = it.get("musicId")
            if not mid or mid in seen:
                continue
            seen.add(mid)
            songs.append({
                "musicId": mid,
                "title": it.get("title"),
                "genre": it.get("genre"),
                "artistIds": it.get("artistIds", []),
                "albumId": it.get("albumId"),
                "fileUrl": it.get("fileUrl"),
                "coverUrl": it.get("coverUrl"),
                "fileName": it.get("fileName"),
                "fileType": it.get("fileType"),
                "fileSize": it.get("fileSize"),
                "createdAt": it.get("createdAt"),
            })

        return response(200, songs)

    except ClientError as e:
        return response(500, {"error": str(e)})

    except Exception as e:
        return response(500, {"error": str(e)})

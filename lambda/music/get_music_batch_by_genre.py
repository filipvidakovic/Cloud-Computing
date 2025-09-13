# lambda/music/get_music_batch_by_genre.py
import json
import os
import decimal
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["MUSIC_TABLE"])
s3c = boto3.client("s3")
S3_BUCKET = os.environ["S3_BUCKET"]

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",  # use exact origin if cookies
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        },
        "body": json.dumps(body, cls=DecimalEncoder, ensure_ascii=False),
    }

def _extract_key_from_url(u: str | None) -> str | None:
    if not u:
        return None
    p = urlparse(u)
    return (p.path or "").lstrip("/") or None

def _presign_from_full_url(u: str | None, expires: int = 3600) -> str | None:
    key = _extract_key_from_url(u)
    if not key:
        return None
    return s3c.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires,
    )

def lambda_handler(event, context):
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    if event.get("httpMethod") != "POST":
        return response(405, {"error": "Method not allowed"})

    try:
        body = json.loads(event.get("body") or "{}")
        genre = body.get("genre")
        music_ids = body.get("musicIds")

        # Validate input
        if not isinstance(genre, str) or not genre.strip():
            return response(400, {"error": "genre (non-empty string) is required"})
        if not isinstance(music_ids, list) or not music_ids:
            return response(400, {"error": "musicIds (non-empty array) is required"})

        # Clean & dedupe IDs, preserve order
        seen = set()
        clean_ids = []
        for mid in music_ids:
            s = str(mid).strip()
            if s and s not in seen:
                seen.add(s)
                clean_ids.append(s)
        if not clean_ids:
            return response(400, {"error": "No valid musicIds after cleaning"})

        # Projection (use names to avoid reserved words)
        projection = (
            "#g, #mid, #alb, #title, #furl, #curl, #aids, #fname, #ftype, #fsize, #created"
        )
        expr_names = {
            "#g": "genre",
            "#mid": "musicId",
            "#alb": "albumId",
            "#title": "title",
            "#furl": "fileUrl",
            "#curl": "coverUrl",
            "#aids": "artistIds",
            "#fname": "fileName",
            "#ftype": "fileType",
            "#fsize": "fileSize",
            "#created": "createdAt",
        }

        # Batch-get in chunks of 100 with retry for UnprocessedKeys
        CHUNK = 100
        found_by_id = {}  # keep last copy; we’ll re-order later

        for i in range(0, len(clean_ids), CHUNK):
            keys = [{"genre": genre, "musicId": mid} for mid in clean_ids[i : i + CHUNK]]
            request = {
                table.name: {
                    "Keys": keys,
                    "ProjectionExpression": projection,
                    "ExpressionAttributeNames": expr_names,
                }
            }

            # Retry up to 5 times for UnprocessedKeys
            for _ in range(5):
                res = dynamodb.batch_get_item(RequestItems=request)
                items = res.get("Responses", {}).get(table.name, [])
                for it in items:
                    mid = it.get("musicId")
                    if mid:
                        found_by_id[mid] = it

                unp = res.get("UnprocessedKeys", {})
                if not unp or not unp.get(table.name, {}).get("Keys"):
                    break
                request = unp  # retry only unprocessed keys

        # Build response in the same order as requested IDs
        songs = []
        for mid in clean_ids:
            it = found_by_id.get(mid)
            if not it:
                continue

            # Presign (recommended). Comment out if you use public files instead.
            file_url = _presign_from_full_url(it.get("fileUrl"))       # ← presign
            cover_url = _presign_from_full_url(it.get("coverUrl")) or it.get("coverUrl")  # ← presign

            songs.append({
                "musicId": mid,
                "title": it.get("title"),
                "genre": it.get("genre"),
                "artistIds": it.get("artistIds", []),
                "albumId": it.get("albumId"),
                "fileUrl": file_url,
                "coverUrl": cover_url,
                "fileName": it.get("fileName"),
                "fileType": it.get("fileType"),
                "fileSize": it.get("fileSize"),
                "createdAt": it.get("createdAt"),
            })

        return response(200, songs)

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return response(500, {"error": f"AWS error: {msg}"})
    except Exception as e:
        return response(500, {"error": str(e)})

import json
import os
import decimal
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse
from boto3.dynamodb.types import TypeDeserializer

dynamodb = boto3.resource("dynamodb")
ddb = boto3.client("dynamodb")  # <-- use client for batch_get_item
s3c = boto3.client("s3")

SONG_TABLE = os.environ["SONG_TABLE"]   # PK: musicId
S3_BUCKET  = os.environ["S3_BUCKET"]

song_table = dynamodb.Table(SONG_TABLE)
_deser = TypeDeserializer()

# --- JSON Decimal encoder ---
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        },
        "body": json.dumps(body, cls=DecimalEncoder, ensure_ascii=False),
    }

def _extract_key_from_url(u: str | None) -> str | None:
    if not u:
        return None
    p = urlparse(u)
    path = (p.path or "").lstrip("/")
    # Handle both virtual-hosted and path-style URLs
    if p.netloc.startswith(f"{S3_BUCKET}.") or p.netloc == S3_BUCKET:
        return path or None
    if path.startswith(f"{S3_BUCKET}/"):
        return path.split("/", 1)[1] or None
    return path or None

def _presign_from_full_url(u: str | None, expires: int = 3600) -> str | None:
    key = _extract_key_from_url(u)
    if not key:
        return None
    return s3c.generate_presigned_url(
        "get_object", Params={"Bucket": S3_BUCKET, "Key": key}, ExpiresIn=expires
    )

def _unmarshal(av_item: dict) -> dict:
    return {k: _deser.deserialize(v) for k, v in av_item.items()}

def lambda_handler(event, context):
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    if event.get("httpMethod") != "POST":
        return response(405, {"error": "Method not allowed"})

    try:
        body = json.loads(event.get("body") or "{}")
        music_ids = body.get("musicIds")

        # Validate input
        if not isinstance(music_ids, list) or not music_ids:
            return response(400, {"error": "musicIds (non-empty array) is required"})

        # Clean & dedupe IDs, preserve order
        seen, clean_ids = set(), []
        for mid in music_ids:
            s = str(mid).strip()
            if s and s not in seen:
                seen.add(s)
                clean_ids.append(s)
        if not clean_ids:
            return response(400, {"error": "No valid musicIds after cleaning"})

        # Projection from SONG_TABLE
        projection = (
            "#mid, #title, #aids, #alb, #furl, #curl, #fname, #ftype, #fsize, "
            "#created, #updated, #genres"
        )
        expr_names = {
            "#mid": "musicId",
            "#title": "title",
            "#aids": "artistIds",
            "#alb": "albumId",
            "#furl": "fileUrl",
            "#curl": "coverUrl",
            "#fname": "fileName",
            "#ftype": "fileType",
            "#fsize": "fileSize",
            "#created": "createdAt",
            "#updated": "updatedAt",
            "#genres": "genres",
        }

        # Batch-get in chunks of 100 with retry for UnprocessedKeys
        CHUNK = 100
        found_by_id: dict[str, dict] = {}

        for i in range(0, len(clean_ids), CHUNK):
            keys = [{"musicId": {"S": mid}} for mid in clean_ids[i:i+CHUNK]]
            request = {
                song_table.name: {
                    "Keys": keys,
                    "ProjectionExpression": projection,
                    "ExpressionAttributeNames": expr_names,
                    # "ConsistentRead": True,  # enable if you need it
                }
            }

            # Retry up to 5 times
            for _ in range(5):
                res = ddb.batch_get_item(RequestItems=request)
                raw_items = res.get("Responses", {}).get(song_table.name, [])
                for av_item in raw_items:
                    item = _unmarshal(av_item)
                    mid = item.get("musicId")
                    if mid:
                        found_by_id[mid] = item

                unp = res.get("UnprocessedKeys", {})
                if not unp or not unp.get(song_table.name, {}).get("Keys"):
                    break
                request = unp  # retry only unprocessed keys

        # Build response in the same order as input IDs
        songs = []
        for mid in clean_ids:
            it = found_by_id.get(mid)
            if not it:
                continue
            file_url  = _presign_from_full_url(it.get("fileUrl"))
            cover_url = _presign_from_full_url(it.get("coverUrl")) or it.get("coverUrl")
            songs.append({
                "musicId": mid,
                "title": it.get("title"),
                "artistIds": it.get("artistIds", []),
                "albumId": it.get("albumId"),
                "fileUrl": file_url,
                "coverUrl": cover_url,
                "fileName": it.get("fileName"),
                "fileType": it.get("fileType"),
                "fileSize": it.get("fileSize"),
                "createdAt": it.get("createdAt"),
                "updatedAt": it.get("updatedAt"),
                "genres": it.get("genres", []),
            })

        return response(200, songs)

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return response(500, {"error": f"AWS error: {msg}"})
    except Exception as e:
        return response(500, {"error": str(e)})

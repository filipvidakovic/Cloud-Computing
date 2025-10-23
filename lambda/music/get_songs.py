import os
import json
import boto3
import decimal
from urllib.parse import urlparse
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

# --- AWS setup ---
s3c = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
ddb = boto3.client("dynamodb")

SONG_TABLE = os.environ["SONG_TABLE"]
S3_BUCKET = os.environ["S3_BUCKET"]

song_table = dynamodb.Table(SONG_TABLE)
_deser = TypeDeserializer()


# --- Helpers ---
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        return super().default(obj)


def response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET"
        },
        "body": json.dumps(body, cls=DecimalEncoder, ensure_ascii=False)
    }


def _extract_key_from_url(u: str | None) -> str | None:
    if not u:
        return None
    p = urlparse(u)
    path = (p.path or "").lstrip("/")
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


# --- Lambda handler ---
def lambda_handler(event, context):
    # Handle CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        params = event.get("queryStringParameters") or {}
        limit = int(params.get("limit", 50))
        last_key_raw = params.get("lastKey")

        scan_kwargs = {"Limit": limit}

        # Handle pagination key
        if last_key_raw:
            try:
                last_key = json.loads(last_key_raw)
                if not isinstance(last_key, dict):
                    raise ValueError("lastKey must be a dict")
                scan_kwargs["ExclusiveStartKey"] = last_key
            except Exception as e:
                return response(400, {"error": f"Invalid lastKey format: {str(e)}"})

        # --- Scan the table ---
        result = song_table.scan(**scan_kwargs)
        raw_items = result.get("Items", [])
        last_evaluated_key = result.get("LastEvaluatedKey")

        # --- Parse and presign data ---
        songs = []
        for it in raw_items:
            if not it.get("musicId") or not it.get("title") or not it.get("fileUrl"):
                continue
            songs.append({
                "musicId": it.get("musicId"),
                "title": it.get("title"),
                "artistIds": it.get("artistIds", []),
                "albumId": it.get("albumId"),
                "fileUrl": _presign_from_full_url(it.get("fileUrl")),
                "coverUrl": _presign_from_full_url(it.get("coverUrl")) or it.get("coverUrl"),
                "fileName": it.get("fileName"),
                "fileType": it.get("fileType"),
                "fileSize": it.get("fileSize"),
                "createdAt": it.get("createdAt"),
                "updatedAt": it.get("updatedAt"),
                "genres": it.get("genres", []),
            })

        return response(200, {
            "songs": songs,
            "lastKey": json.dumps(last_evaluated_key, cls=DecimalEncoder) if last_evaluated_key else None
        })

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return response(500, {"error": f"AWS error: {msg}"})
    except Exception as e:
        return response(500, {"error": str(e)})

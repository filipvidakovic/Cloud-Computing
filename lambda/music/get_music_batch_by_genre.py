import json
import os
import decimal
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse
from boto3.dynamodb.types import TypeDeserializer

# --- AWS clients/resources ---
dynamodb = boto3.resource("dynamodb")
ddb = boto3.client("dynamodb")          # low-level client
s3c = boto3.client("s3")

# --- Env ---
SONG_TABLE = os.environ["SONG_TABLE"]   # PK: musicId
RATES_TABLE = os.environ["RATES_TABLE"] # PK: userId, SK: musicId
S3_BUCKET  = os.environ["S3_BUCKET"]

song_table = dynamodb.Table(SONG_TABLE)
rate_table = dynamodb.Table(RATES_TABLE)
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
            "Access-Control-Allow-Methods": "OPTIONS,POST",
        },
        "body": json.dumps(body, cls=DecimalEncoder, ensure_ascii=False),
    }

def _extract_key_from_url(u: str | None) -> str | None:
    if not u:
        return None
    p = urlparse(u)
    path = (p.path or "").lstrip("/")

    # virtual-hosted or path-style
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

def get_user_id(event):
    rc = event.get("requestContext", {})
    auth = rc.get("authorizer", {})
    # Cognito/Lambda authorizer (REQUEST) with JWT claims
    if "claims" in auth:
        return auth["claims"].get("sub")
    return None

def batch_get_rates(user_id: str | None, music_ids: list[str]) -> dict[str, str | None]:
    """
    Batch-get user rates for given musicIds from RATES_TABLE.
    Uses the low-level client and unmarshals results.
    """
    if not user_id or not music_ids:
        return {}

    CHUNK = 100
    out: dict[str, str | None] = {}

    for i in range(0, len(music_ids), CHUNK):
        key_chunk = music_ids[i:i+CHUNK]
        req = {
            rate_table.name: {
                "Keys": [
                    {"userId": {"S": user_id}, "musicId": {"S": mid}}
                    for mid in key_chunk
                ]
            }
        }
        # retry UnprocessedKeys up to 5x
        for _ in range(5):
            res = ddb.batch_get_item(RequestItems=req)
            items = res.get("Responses", {}).get(rate_table.name, [])
            for av in items:
                it = _unmarshal(av)
                mid = it.get("musicId")
                if mid:
                    out[mid] = it.get("rate")  # may be None

            unp = res.get("UnprocessedKeys", {})
            if not unp or not unp.get(rate_table.name, {}).get("Keys"):
                break
            req = unp

    return out

def lambda_handler(event, context):
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    if event.get("httpMethod") != "POST":
        return response(405, {"error": "Method not allowed"})

    try:
        body = json.loads(event.get("body") or "{}")
        music_ids = body.get("musicIds")

        if not isinstance(music_ids, list) or not music_ids:
            return response(400, {"error": "musicIds (non-empty array) is required"})

        # Clean & dedupe IDs (preserve order)
        seen, clean_ids = set(), []
        for mid in music_ids:
            s = str(mid).strip()
            if s and s not in seen:
                seen.add(s)
                clean_ids.append(s)
        if not clean_ids:
            return response(400, {"error": "No valid musicIds after cleaning"})

        # ---- Batch-get songs from SONG_TABLE via client ----
        projection = (
            "#mid,#title,#aids,#alb,#furl,#curl,#fname,#ftype,#fsize,#created,#updated,#genres"
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

        found_by_id: dict[str, dict] = {}
        CHUNK = 100

        for i in range(0, len(clean_ids), CHUNK):
            keys = [{"musicId": {"S": mid}} for mid in clean_ids[i:i+CHUNK]]
            request = {
                song_table.name: {
                    "Keys": keys,
                    "ProjectionExpression": projection,
                    "ExpressionAttributeNames": expr_names,
                }
            }
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
                request = unp

        # ---- Rates for this user (optional) ----
        user_id = get_user_id(event)
        rates = batch_get_rates(user_id, clean_ids)

        # ---- Build response in requested order ----
        songs = []
        for mid in clean_ids:
            it = found_by_id.get(mid)
            if not it:
                continue
            songs.append({
                "musicId": mid,
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
                "rate": rates.get(mid),
            })

        return response(200, songs)

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return response(500, {"error": f"AWS error: {msg}"})
    except Exception as e:
        return response(500, {"error": str(e)})

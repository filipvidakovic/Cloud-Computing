import json
import os
import decimal
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse
from boto3.dynamodb.types import TypeDeserializer

# --- AWS clients/resources ---
dynamodb = boto3.resource("dynamodb")
ddb = boto3.client("dynamodb")          # low-level client for batch_get_item
s3c = boto3.client("s3")

# --- Env ---
ARTIST_INFO_TABLE = os.environ["ARTIST_INFO_TABLE"]  # PK: artistId
SONG_TABLE        = os.environ["SONG_TABLE"]         # PK: musicId
RATES_TABLE       = os.environ["RATES_TABLE"]        # PK: userId, SK: musicId
S3_BUCKET         = os.environ["S3_BUCKET"]

artist_info_table = dynamodb.Table(ARTIST_INFO_TABLE)
song_table = dynamodb.Table(SONG_TABLE)
rate_table = dynamodb.Table(RATES_TABLE)
_deser = TypeDeserializer()

# --- JSON Decimal encoder (same as your other lambda) ---
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
            "Access-Control-Allow-Methods": "OPTIONS,GET",
        },
        "body": json.dumps(body, cls=DecimalEncoder, ensure_ascii=False),
    }

def _unmarshal(av_item: dict) -> dict:
    return {k: _deser.deserialize(v) for k, v in av_item.items()}

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
    Matches your other lambda's behavior.
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

    if event.get("httpMethod") != "GET":
        return response(405, {"error": "Method not allowed"})

    try:
        # artistId from path / query
        path_params = event.get("pathParameters") or {}
        qs = event.get("queryStringParameters") or {}

        artist_id = path_params.get("artistId") or (qs or {}).get("artistId")
        if not artist_id:
            return response(400, {"error": "artistId is required"})

        # 1) Load artist profile to get list of musicIds
        info = artist_info_table.get_item(Key={"artistId": artist_id}).get("Item")
        if not info:
            return response(404, {"error": "Artist not found"})

        raw_ids = info.get("songs") or []
        # Clean + dedupe + preserve original order
        seen, music_ids = set(), []
        for mid in raw_ids:
            s = str(mid).strip()
            if s and s not in seen:
                seen.add(s)
                music_ids.append(s)

        if not music_ids:
            return response(200, [])  # same shape as other lambda: empty array

        # 2) Batch-get songs from SONG_TABLE (same fields and order as your other lambda)
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
        for i in range(0, len(music_ids), CHUNK):
            keys = [{"musicId": {"S": mid}} for mid in music_ids[i:i+CHUNK]]
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

        # 3) User-specific rates (optional, same behavior as the other lambda)
        user_id = get_user_id(event)
        rates = batch_get_rates(user_id, music_ids)

        # 4) Build the response array in the same order as music_ids
        songs = []
        for mid in music_ids:
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

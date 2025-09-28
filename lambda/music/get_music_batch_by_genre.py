# lambda/music/get_music_batch_by_genre.py
import json
import os
import decimal
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["MUSIC_TABLE"])
rate_table = dynamodb.Table(os.environ["RATES_TABLE"])
s3c = boto3.client("s3")
S3_BUCKET = os.environ["S3_BUCKET"]

COGNITO_REGION = os.environ.get("COGNITO_REGION", "eu-central-1")
USERPOOL_ID = os.environ.get("USERPOOL_ID", "eu-central-1_xxxxxxxx")
CLIENT_ID = os.environ.get("COGNITO_CLIENT_ID")

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

def get_user_id(event):
    rc = event.get("requestContext", {})
    auth = rc.get("authorizer", {})
    if "claims" in auth:   
        return auth["claims"].get("sub")
    return None

def batch_get_reactions(user_id: str, music_ids: list[str]) -> dict[str, str]:
    """Fetch reactions from Rates table for given user/musicIds."""
    if not user_id:
        return {}

    CHUNK = 100
    reactions = {}

    for i in range(0, len(music_ids), CHUNK):
        keys = [{"userId": user_id, "musicId": mid} for mid in music_ids[i:i+CHUNK]]
        request = {rate_table.name: {"Keys": keys}}
        for _ in range(5):
            res = dynamodb.batch_get_item(RequestItems=request)
            items = res.get("Responses", {}).get(rate_table.name, [])
            for it in items:
                mid = it.get("musicId")
                reaction = it.get("reaction")  # assuming column name = reaction
                if mid and reaction:
                    reactions[mid] = reaction

            unp = res.get("UnprocessedKeys", {})
            if not unp or not unp.get(rate_table.name, {}).get("Keys"):
                break
            request = unp

    return reactions

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

        if not isinstance(genre, str) or not genre.strip():
            return response(400, {"error": "genre (non-empty string) is required"})
        if not isinstance(music_ids, list) or not music_ids:
            return response(400, {"error": "musicIds (non-empty array) is required"})

        # clean ids
        seen, clean_ids = set(), []
        for mid in music_ids:
            s = str(mid).strip()
            if s and s not in seen:
                seen.add(s)
                clean_ids.append(s)
        if not clean_ids:
            return response(400, {"error": "No valid musicIds after cleaning"})

        # batch-get from MUSIC_TABLE
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

        found_by_id = {}
        CHUNK = 100
        for i in range(0, len(clean_ids), CHUNK):
            keys = [{"genre": genre, "musicId": mid} for mid in clean_ids[i : i + CHUNK]]
            request = {
                table.name: {
                    "Keys": keys,
                    "ProjectionExpression": projection,
                    "ExpressionAttributeNames": expr_names,
                }
            }
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
                request = unp

        # get reactions for this user
        user_id = get_user_id(event)
        reactions = batch_get_reactions(user_id, clean_ids)

        # Build response
        songs = []
        for mid in clean_ids:
            it = found_by_id.get(mid)
            if not it:
                continue
            file_url = _presign_from_full_url(it.get("fileUrl"))
            cover_url = _presign_from_full_url(it.get("coverUrl")) or it.get("coverUrl")

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
                "reaction": reactions.get(mid, None),  # 👈 love/like/dislike/null
            })

        return response(200, songs)

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return response(500, {"error": f"AWS error: {msg}"})
    except Exception as e:
        return response(500, {"error": str(e)})

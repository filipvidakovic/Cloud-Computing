import json
import os
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
from urllib.parse import urlparse

# ---- Env ----
SONG_TABLE = os.environ.get("SONG_TABLE", "SongTable")          # PK: musicId
MUSIC_BY_GENRE_TABLE = os.environ.get("MUSIC_BY_GENRE_TABLE", "MusicByGenre")  # PK: genre, SK: musicId
S3_BUCKET = os.environ["S3_BUCKET"]

# ---- AWS clients ----
dynamodb = boto3.resource("dynamodb")
dynamo_client = boto3.client("dynamodb")
s3 = boto3.client("s3")

song_table = dynamodb.Table(SONG_TABLE)
genre_table = dynamodb.Table(MUSIC_BY_GENRE_TABLE)


def response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,DELETE,PUT",
        },
        "body": json.dumps(body),
    }


def _extract_s3_key(u: str | None) -> str | None:
    if not u:
        return None
    p = urlparse(u)
    key = (p.path or "").lstrip("/")
    return key or None


def _scan_index_rows_for_music(music_id: str):
    """
    Return all rows from MUSIC_BY_GENRE_TABLE with the given musicId.
    Uses Scan + FilterExpression (no GSI required). Paginates until done.
    """
    items = []
    scan_kwargs = {
        "FilterExpression": Attr("musicId").eq(music_id),
        "ProjectionExpression": "#g,#m",
        "ExpressionAttributeNames": {"#g": "genre", "#m": "musicId"},
        "ConsistentRead": True,  # reduce chance of racing with recent writes
    }
    res = genre_table.scan(**scan_kwargs)
    items.extend(res.get("Items", []))
    while "LastEvaluatedKey" in res:
        res = genre_table.scan(ExclusiveStartKey=res["LastEvaluatedKey"], **scan_kwargs)
        items.extend(res.get("Items", []))
    return items


def lambda_handler(event, context):
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    if event.get("httpMethod") not in ("DELETE", "POST"):
        return response(405, {"error": "Method not allowed. Use DELETE or POST."})

    try:
        # Accept musicId from query or JSON body
        params = event.get("queryStringParameters") or {}
        music_id = params.get("musicId")
        if not music_id:
            body = json.loads(event.get("body") or "{}")
            music_id = body.get("musicId")

        if not music_id:
            return response(400, {"error": "Parameter 'musicId' is required"})

        # 1) Fetch canonical song (to pick up file/cover URLs)
        song_item = song_table.get_item(Key={"musicId": music_id}, ConsistentRead=True).get("Item")

        # 2) Find all (genre, musicId) rows via Scan (no GSI needed)
        index_rows = _scan_index_rows_for_music(music_id)

        if not song_item and not index_rows:
            return response(404, {"error": f"No records found for musicId: {music_id}"})

        # 3) Build transactional deletes (chunk if >25)
        transact_items = []
        if song_item:
            transact_items.append({
                "Delete": {
                    "TableName": SONG_TABLE,
                    "Key": {"musicId": {"S": music_id}},
                    "ReturnValuesOnConditionCheckFailure": "NONE",
                }
            })

        for it in index_rows:
            g = it.get("genre")
            if not g:
                continue
            transact_items.append({
                "Delete": {
                    "TableName": MUSIC_BY_GENRE_TABLE,
                    "Key": {"genre": {"S": g}, "musicId": {"S": music_id}},
                    "ReturnValuesOnConditionCheckFailure": "NONE",
                }
            })

        if transact_items:
            if len(transact_items) <= 25:
                dynamo_client.transact_write_items(TransactItems=transact_items)
            else:
                # delete the song first (if present)
                start = 0
                if song_item:
                    dynamo_client.transact_write_items(TransactItems=[transact_items[0]])
                    start = 1
                for i in range(start, len(transact_items), 25):
                    dynamo_client.transact_write_items(TransactItems=transact_items[i:i+25])

        # 4) Best-effort S3 deletes (audio & cover from SONG_TABLE)
        deleted_files, deleted_covers = [], []
        if song_item:
            fkey = _extract_s3_key(song_item.get("fileUrl"))
            if fkey:
                try:
                    s3.delete_object(Bucket=S3_BUCKET, Key=fkey)
                    deleted_files.append(fkey)
                except Exception as e:
                    print(f"Could not delete music file: {e}")

            ckey = _extract_s3_key(song_item.get("coverUrl"))
            if ckey:
                try:
                    s3.delete_object(Bucket=S3_BUCKET, Key=ckey)
                    deleted_covers.append(ckey)
                except Exception as e:
                    print(f"Could not delete cover image: {e}")

        return response(200, {
            "message": "Delete completed",
            "musicId": music_id,
            "deletedSong": bool(song_item),
            "deletedIndexRows": len(index_rows),
            "deletedS3Files": deleted_files,
            "deletedCoverImages": deleted_covers,
        })

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return response(500, {"error": f"AWS error: {msg}"})
    except Exception as e:
        return response(500, {"error": str(e)})

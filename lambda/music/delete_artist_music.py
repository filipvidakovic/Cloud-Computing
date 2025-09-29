import json
import os
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
from urllib.parse import urlparse

# ---- Env ----
SONG_TABLE = os.environ.get("SONG_TABLE", "SongTable")                 # PK: musicId
MUSIC_BY_GENRE_TABLE = os.environ.get("MUSIC_BY_GENRE_TABLE", "MusicByGenre")  # PK: genre, SK: musicId
S3_BUCKET = os.environ["S3_BUCKET"]

# ---- AWS clients ----
dynamodb = boto3.resource("dynamodb")
dynamo_client = boto3.client("dynamodb")
s3 = boto3.client("s3")

song_table = dynamodb.Table(SONG_TABLE)
genre_table = dynamodb.Table(MUSIC_BY_GENRE_TABLE)


def response(status_code, body):
    return {
        "statusCode": status_code,
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
    """Return all rows from MUSIC_BY_GENRE_TABLE with the given musicId.
    Uses Scan + FilterExpression (no GSI). Paginates until done.
    """
    items = []
    scan_kwargs = {
        "FilterExpression": Attr("musicId").eq(music_id),
        "ProjectionExpression": "#g,#m",
        "ExpressionAttributeNames": {"#g": "genre", "#m": "musicId"},
        "ConsistentRead": True,
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
        # artistId from path (primary), or fallback to query/body
        artist_id = (event.get("pathParameters") or {}).get("artistId")
        if not artist_id:
            artist_id = (event.get("queryStringParameters") or {}).get("artistId")
        if not artist_id:
            artist_id = (json.loads(event.get("body") or "{}") or {}).get("artistId")

        if not artist_id:
            return response(400, {"error": "Missing artistId"})

        # 1) Scan SONG_TABLE for songs that contain this artistId
        songs = []
        scan_kwargs = {
            "FilterExpression": Attr("artistIds").contains(artist_id),
            "ConsistentRead": True,
        }
        res = song_table.scan(**scan_kwargs)
        songs.extend(res.get("Items", []))
        while "LastEvaluatedKey" in res:
            res = song_table.scan(ExclusiveStartKey=res["LastEvaluatedKey"], **scan_kwargs)
            songs.extend(res.get("Items", []))

        if not songs:
            return response(200, {
                "message": f"No songs found for artist {artist_id}",
                "deletedSongs": [],
                "deletedIndexRows": 0,
                "deletedS3Files": [],
                "deletedCoverImages": []
            })

        deleted_music_ids = []
        deleted_index_rows_total = 0
        deleted_files = []
        deleted_covers = []

        # 2) For each song, delete the song row, its genre index rows, and S3 objects
        for song in songs:
            music_id = song.get("musicId")
            if not music_id:
                continue

            # 2a) Find all (genre, musicId) rows (no GSI)
            index_rows = _scan_index_rows_for_music(music_id)

            # 2b) Build transactional deletes for this song
            tx = [{
                "Delete": {
                    "TableName": SONG_TABLE,
                    "Key": {"musicId": {"S": music_id}},
                    "ReturnValuesOnConditionCheckFailure": "NONE",
                }
            }]
            for it in index_rows:
                g = it.get("genre")
                if not g:
                    continue
                tx.append({
                    "Delete": {
                        "TableName": MUSIC_BY_GENRE_TABLE,
                        "Key": {"genre": {"S": g}, "musicId": {"S": music_id}},
                        "ReturnValuesOnConditionCheckFailure": "NONE",
                    }
                })

            # Execute (chunk if >25)
            if len(tx) <= 25:
                dynamo_client.transact_write_items(TransactItems=tx)
            else:
                # delete song row first, then index rows in chunks
                dynamo_client.transact_write_items(TransactItems=[tx[0]])
                for i in range(1, len(tx), 25):
                    dynamo_client.transact_write_items(TransactItems=tx[i:i+25])

            # 2c) Best-effort delete S3 objects referenced by the song
            fkey = _extract_s3_key(song.get("fileUrl"))
            if fkey:
                try:
                    s3.delete_object(Bucket=S3_BUCKET, Key=fkey)
                    deleted_files.append(fkey)
                except Exception as e:
                    pass

            ckey = _extract_s3_key(song.get("coverUrl"))
            if ckey:
                try:
                    s3.delete_object(Bucket=S3_BUCKET, Key=ckey)
                    deleted_covers.append(ckey)
                except Exception as e:
                    pass

            deleted_music_ids.append(music_id)
            deleted_index_rows_total += len(index_rows)

        return response(200, {
            "message": f"Deleted {len(deleted_music_ids)} songs for artist {artist_id}",
            "deletedSongs": deleted_music_ids,
            "deletedIndexRows": deleted_index_rows_total,
            "deletedS3Files": deleted_files,
            "deletedCoverImages": deleted_covers
        })

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return response(500, {"error": f"AWS error: {msg}"})
    except Exception as e:
        return response(500, {"error": str(e)})

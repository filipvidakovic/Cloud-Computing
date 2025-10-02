# lambda/artists/delete_artist.py
import json
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from urllib.parse import urlparse

# --- Env (artist tables) ---
ARTISTS_TABLE = os.environ["ARTISTS_TABLE"]           # PK: artistId, SK: genre
ARTIST_INFO_TABLE = os.environ["ARTIST_INFO_TABLE"]   # PK: artistId

# --- Env (music tables + s3) ---
SONG_TABLE = os.environ["SONG_TABLE"]                 # PK: musicId
MUSIC_BY_GENRE_TABLE = os.environ["MUSIC_BY_GENRE_TABLE"]  # PK: genre, SK: musicId
S3_BUCKET = os.environ["S3_BUCKET"]

# --- AWS clients ---
dynamodb = boto3.resource("dynamodb")
dynamo_client = boto3.client("dynamodb")
s3 = boto3.client("s3")

artist_table = dynamodb.Table(ARTISTS_TABLE)
info_table = dynamodb.Table(ARTIST_INFO_TABLE)
song_table = dynamodb.Table(SONG_TABLE)
genre_table = dynamodb.Table(MUSIC_BY_GENRE_TABLE)

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,DELETE"
        },
        "body": json.dumps(body),
    }

def _query_all_artist_rows(artist_id: str):
    items, lek = [], None
    while True:
        kwargs = {"KeyConditionExpression": Key("artistId").eq(artist_id)}
        if lek:
            kwargs["ExclusiveStartKey"] = lek
        resp = artist_table.query(**kwargs)
        items.extend(resp.get("Items", []))
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
    return items

def _extract_s3_key(u: str | None) -> str | None:
    if not u:
        return None
    p = urlparse(u)
    key = (p.path or "").lstrip("/")
    return key or None

def _scan_index_rows_for_music(music_id: str):
    """Return all rows from MUSIC_BY_GENRE_TABLE with the given musicId (no GSI)."""
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

def _delete_songs_for_artist(artist_id: str):
    """Scan SONG_TABLE for items where artistIds contains artist_id, and delete:
       - the SONG_TABLE row
       - all MUSIC_BY_GENRE rows for that musicId
       - the fileUrl and coverUrl objects in S3 (best effort)
    """
    # 1) find all songs with this artistId
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
        return {
            "message": f"No songs found for artist {artist_id}",
            "deletedSongs": [],
            "deletedIndexRows": 0,
            "deletedS3Files": [],
            "deletedCoverImages": []
        }

    deleted_music_ids = []
    deleted_index_rows_total = 0
    deleted_files = []
    deleted_covers = []

    # 2) delete each song + its genre index rows + S3 files
    for song in songs:
        music_id = song.get("musicId")
        if not music_id:
            continue

        index_rows = _scan_index_rows_for_music(music_id)

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

        # execute (chunk if >25)
        if len(tx) <= 25:
            dynamo_client.transact_write_items(TransactItems=tx)
        else:
            dynamo_client.transact_write_items(TransactItems=[tx[0]])
            for i in range(1, len(tx), 25):
                dynamo_client.transact_write_items(TransactItems=tx[i:i+25])

        # best-effort S3 deletion
        fkey = _extract_s3_key(song.get("fileUrl"))
        if fkey:
            try:
                s3.delete_object(Bucket=S3_BUCKET, Key=fkey)
                deleted_files.append(fkey)
            except Exception:
                pass
        ckey = _extract_s3_key(song.get("coverUrl"))
        if ckey:
            try:
                s3.delete_object(Bucket=S3_BUCKET, Key=ckey)
                deleted_covers.append(ckey)
            except Exception:
                pass

        deleted_music_ids.append(music_id)
        deleted_index_rows_total += len(index_rows)

    return {
        "message": f"Deleted {len(deleted_music_ids)} songs for artist {artist_id}",
        "deletedSongs": deleted_music_ids,
        "deletedIndexRows": deleted_index_rows_total,
        "deletedS3Files": deleted_files,
        "deletedCoverImages": deleted_covers
    }

def lambda_handler(event, context):
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        path_params = event.get("pathParameters") or {}
        body = json.loads(event.get("body", "{}"))

        artist_id = path_params.get("artistId") or body.get("artistId")
        if not artist_id:
            return response(400, {"error": "artistId is required"})

        # ensure artist exists
        profile = info_table.get_item(Key={"artistId": artist_id}).get("Item")
        if not profile:
            return response(404, {"error": "Artist not found"})

        # 1) delete rows from ARTISTS_TABLE (artistId, genre)
        items = _query_all_artist_rows(artist_id)
        with artist_table.batch_writer() as batch:
            for it in items:
                batch.delete_item(Key={"artistId": it["artistId"], "genre": it["genre"]})

        # 2) delete profile from ARTIST_INFO_TABLE
        info_table.delete_item(Key={"artistId": artist_id})

        # 3) delete all songs for this artist (inline)
        songs_result = _delete_songs_for_artist(artist_id)

        return response(200, {
            "message": f"Artist {artist_id} deleted successfully.",
            "songsCleanup": songs_result
        })

    except ClientError as e:
        return response(500, {"error": str(e)})
    except Exception as e:
        return response(500, {"error": str(e)})

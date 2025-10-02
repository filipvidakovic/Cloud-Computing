import json
import os
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
from urllib.parse import urlparse
from typing import List, Dict, Any
from common.queue import enqueue_recompute
from boto3.dynamodb.conditions import Key

SONG_TABLE = os.environ.get("SONG_TABLE", "SongTable")
MUSIC_BY_GENRE_TABLE = os.environ.get("MUSIC_BY_GENRE_TABLE", "MusicByGenre")
S3_BUCKET = os.environ["S3_BUCKET"]
ARTIST_INFO_TABLE = os.environ["ARTIST_INFO_TABLE"]

dynamodb = boto3.resource("dynamodb")
dynamo_client = boto3.client("dynamodb")
s3 = boto3.client("s3")

song_table = dynamodb.Table(SONG_TABLE)
genre_table = dynamodb.Table(MUSIC_BY_GENRE_TABLE)
artist_info_table = dynamodb.Table(ARTIST_INFO_TABLE)
subs_table = dynamodb.Table(os.environ["USER_SUBSCRIPTIONS_TABLE"])



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
    # works for both virtual-hosted and path-style URLs
    path = (p.path or "").lstrip("/")
    if not path:
        return None
    # if the URL is path-style and starts with bucket name, strip it
    if path.startswith(f"{S3_BUCKET}/"):
        return path.split("/", 1)[1] or None
    return path


def _scan_index_rows_for_music(music_id: str) -> List[Dict[str, Any]]:
    """
    Fallback: return all rows from MUSIC_BY_GENRE_TABLE with the given musicId.
    Uses Scan + FilterExpression (no GSI required). Paginates until done.
    """
    items: List[Dict[str, Any]] = []
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


def _build_txn_deletes(music_id: str, genres: List[str], include_song_delete: bool, max_per_batch: int = 25) -> List[List[Dict[str, Any]]]:
    """
    Build TransactWriteItems batches (lists of <= max_per_batch) to:
      - delete song row (optional)
      - delete each (genre, musicId) row
    Returns list of batches.
    """
    ops: List[Dict[str, Any]] = []

    if include_song_delete:
        ops.append({
            "Delete": {
                "TableName": SONG_TABLE,
                "Key": {"musicId": {"S": music_id}},
                "ReturnValuesOnConditionCheckFailure": "NONE",
            }
        })

    # de-dupe & sanitize genres
    clean_genres = []
    seen = set()
    for g in genres or []:
        s = str(g).strip()
        if s and s not in seen:
            seen.add(s)
            clean_genres.append(s)

    for g in clean_genres:
        ops.append({
            "Delete": {
                "TableName": MUSIC_BY_GENRE_TABLE,
                "Key": {"genre": {"S": g}, "musicId": {"S": music_id}},
                "ReturnValuesOnConditionCheckFailure": "NONE",
            }
        })

    # chunk into batches
    batches: List[List[Dict[str, Any]]] = []
    for i in range(0, len(ops), max_per_batch):
        batches.append(ops[i:i + max_per_batch])
    return batches


def lambda_handler(event, context):
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    if event.get("httpMethod") not in ("DELETE", "POST"):
        return response(405, {"error": "Method not allowed. Use DELETE or POST."})

    try:
        params = event.get("queryStringParameters") or {}
        music_id = params.get("musicId")
        if not music_id:
            body = json.loads(event.get("body") or "{}")
            music_id = body.get("musicId")

        if not music_id:
            return response(400, {"error": "Parameter 'musicId' is required"})

        song_res = song_table.get_item(Key={"musicId": music_id}, ConsistentRead=True)
        song_item = song_res.get("Item")

        artist_ids = []
        if song_item:
            artist_ids = song_item.get("artistIds", [])
            if isinstance(artist_ids, list):
                artist_ids = [a for a in artist_ids if isinstance(a, str)]

        genres_from_song = []
        if song_item:
            g = song_item.get("genres")
            if isinstance(g, list):
                genres_from_song = [str(x) for x in g if isinstance(x, (str, bytes)) or x is not None]

        if genres_from_song:
            index_rows_count = len(set([s.strip() for s in genres_from_song if str(s).strip()]))
            used_fallback_scan = False
        else:
            idx_rows = _scan_index_rows_for_music(music_id)
            genres_from_song = [it.get("genre") for it in idx_rows if it.get("genre")]
            index_rows_count = len(idx_rows)
            used_fallback_scan = True

        if not song_item and index_rows_count == 0:
            return response(404, {"error": f"No records found for musicId: {music_id}"})

        batches = _build_txn_deletes(
            music_id=music_id,
            genres=genres_from_song,
            include_song_delete=bool(song_item),
            max_per_batch=25
        )

        for batch in batches:
            dynamo_client.transact_write_items(TransactItems=batch)

        # remove song from each artist's songs list
        for artist_id in artist_ids:
            try:
                res = artist_info_table.get_item(Key={"artistId": artist_id})
                if "Item" not in res:
                    continue
                songs = res["Item"].get("songs", [])
                if not isinstance(songs, list):
                    songs = []
                new_songs = [s for s in songs if s != music_id]
                artist_info_table.update_item(
                    Key={"artistId": artist_id},
                    UpdateExpression="SET #songs = :s",
                    ExpressionAttributeNames={"#songs": "songs"},
                    ExpressionAttributeValues={":s": new_songs}
                )
            except Exception as e:
                print(f"Failed to update artist {artist_id}: {e}")

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


        # --- Recompute feed for subscribed users ---
        try:
            # notify users subscribed by genre
            for g in genres_from_song:
                resp = subs_table.query(
                    IndexName="SubscriptionTypeTargetIdIndex",
                    KeyConditionExpression=Key("subscriptionType").eq("genre") & Key("targetId").eq(g)
                )
                for item in resp.get("Items", []):
                    user_id = item["userId"]
                    enqueue_recompute(user_id, "delete_song_genre", music_id)

            # notify users subscribed by artist
            for artist_id in artist_ids:
                resp = subs_table.query(
                    IndexName="SubscriptionTypeTargetIdIndex",
                    KeyConditionExpression=Key("subscriptionType").eq("artist") & Key("targetId").eq(artist_id)
                )
                for item in resp.get("Items", []):
                    user_id = item["userId"]
                    enqueue_recompute(user_id, "delete_song_artist", music_id)

        except Exception as e:
            print(f"⚠️ Failed to enqueue recompute jobs on delete: {e}")

        return response(200, {
            "message": "Delete completed",
            "musicId": music_id,
            "deletedSong": bool(song_item),
            "deletedIndexRows": index_rows_count,
            "indexDeletionSource": "song.genres" if not used_fallback_scan else "scan",
            "deletedS3Files": deleted_files,
            "deletedCoverImages": deleted_covers,
        })

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return response(500, {"error": f"AWS error: {msg}"})
    except Exception as e:
        return response(500, {"error": str(e)})

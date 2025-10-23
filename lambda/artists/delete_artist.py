# lambda/artists/delete_artist.py
import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# --- Env (artist tables) ---
ARTISTS_TABLE = os.environ["ARTISTS_TABLE"]            # PK: artistId, SK: genre
ARTIST_INFO_TABLE = os.environ["ARTIST_INFO_TABLE"]    # PK: artistId

# --- Env (music tables) ---
SONG_TABLE = os.environ["SONG_TABLE"]                  # PK: musicId
MUSIC_BY_GENRE_TABLE = os.environ["MUSIC_BY_GENRE_TABLE"]  # PK: genre, SK: musicId

# --- AWS clients ---
dynamodb = boto3.resource("dynamodb")
artist_table = dynamodb.Table(ARTISTS_TABLE)
info_table = dynamodb.Table(ARTIST_INFO_TABLE)
song_table = dynamodb.Table(SONG_TABLE)
music_table = dynamodb.Table(MUSIC_BY_GENRE_TABLE)

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,PUT,DELETE",
        },
        "body": json.dumps(body),
    }

def _query_all_artist_rows(artist_id: str):
    """Get all (artistId, genre) rows for this artist from ARTISTS_TABLE."""
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

def _delete_song_and_index(music_id: str) -> dict:
    """
    Delete one song from SONG_TABLE and its (genre, musicId) rows from MUSIC_BY_GENRE_TABLE.
    We avoid scans by first reading the song to get its 'genres' attribute.
    """
    # Get the song once to know its genres (avoid table scan)
    song_resp = song_table.get_item(Key={"musicId": music_id})
    song_item = song_resp.get("Item")
    if not song_item:
        # Nothing to delete in SONG_TABLE; also no index rows since we don't know genres.
        return {"musicId": music_id, "deletedSong": False, "deletedIndex": 0}

    genres = song_item.get("genres") or []   # stored as a list of strings in your uploader
    if not isinstance(genres, list):
        genres = []

    # Delete the SONG_TABLE row
    song_table.delete_item(Key={"musicId": music_id})

    # Delete each (genre, musicId) in MUSIC_BY_GENRE_TABLE
    deleted_idx = 0
    for g in genres:
        if not g:
            continue
        music_table.delete_item(Key={"genre": g, "musicId": music_id})
        deleted_idx += 1

    return {"musicId": music_id, "deletedSong": True, "deletedIndex": deleted_idx}

def lambda_handler(event, context):
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        # artistId from path / query / body
        path_params = event.get("pathParameters") or {}
        qs = event.get("queryStringParameters") or {}
        raw = event.get("body") or "{}"
        try:
            body = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception:
            body = {}

        artist_id = path_params.get("artistId") or qs.get("artistId") or body.get("artistId")
        if not artist_id:
            return response(400, {"error": "artistId is required"})

        # Ensure artist exists and fetch profile (to get songs list)
        info_resp = info_table.get_item(Key={"artistId": artist_id})
        profile = info_resp.get("Item")
        if not profile:
            return response(404, {"error": "Artist not found"})

        songs_list = profile.get("songs") or []   # list of musicIds
        if not isinstance(songs_list, list):
            songs_list = []

        # 1) Delete songs referenced by artist_info.songs
        per_song_results = []
        for music_id in songs_list:
            if not music_id:
                continue
            per_song_results.append(_delete_song_and_index(music_id))

        # 2) Delete rows in ARTISTS_TABLE (artistId, genre)
        items = _query_all_artist_rows(artist_id)
        with artist_table.batch_writer() as batch:
            for it in items:
                aid = it.get("artistId")
                g = it.get("genre")
                if aid and g:
                    batch.delete_item(Key={"artistId": aid, "genre": g})

        # 3) Delete ARTIST_INFO_TABLE record
        info_table.delete_item(Key={"artistId": artist_id})

        deleted_index_total = sum(r.get("deletedIndex", 0) for r in per_song_results)
        deleted_songs_total = sum(1 for r in per_song_results if r.get("deletedSong"))

        return response(200, {
            "message": f"Artist {artist_id} and related songs deleted.",
            "deletedSongs": deleted_songs_total,
            "deletedIndexRows": deleted_index_total,
            "songResults": per_song_results,   # optional detail; remove if too verbose
        })

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return response(500, {"error": msg})
    except Exception as e:
        return response(500, {"error": str(e)})

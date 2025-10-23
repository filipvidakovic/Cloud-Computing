import json
import os
from typing import List, Dict, Any
import boto3
from botocore.exceptions import ClientError

# ---- Env ----
SONG_TABLE = os.environ["SONG_TABLE"]                 # PK: musicId
MUSIC_BY_GENRE_TABLE = os.environ["MUSIC_BY_GENRE_TABLE"]  # PK: genre, SK: musicId
ARTIST_INFO_TABLE = os.environ["ARTIST_INFO_TABLE"]   # PK: artistId

# ---- AWS ----
dynamodb = boto3.resource("dynamodb")
ddb = boto3.client("dynamodb")

song_table = dynamodb.Table(SONG_TABLE)
music_by_genre_table = dynamodb.Table(MUSIC_BY_GENRE_TABLE)
artist_info_table = dynamodb.Table(ARTIST_INFO_TABLE)

def response(status: int, body: Any):
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
        },
        "body": json.dumps(body),
    }

def _chunked(seq, n):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) == n:
            yield buf
            buf = []
    if buf:
        yield buf

def _load_song(music_id: str) -> Dict[str, Any] | None:
    return song_table.get_item(Key={"musicId": music_id}).get("Item")

def _delete_song_and_index(music_id: str, genres: List[str]) -> int:
    """Delete the SONG_TABLE row and all (genre, musicId) rows in MUSIC_BY_GENRE_TABLE.
       Returns how many index rows were deleted.
    """
    # delete song row
    song_table.delete_item(Key={"musicId": music_id})

    deleted_idx = 0
    for g in genres or []:
        if not g:
            continue
        music_by_genre_table.delete_item(Key={"genre": g, "musicId": music_id})
        deleted_idx += 1
    return deleted_idx

def _remove_music_from_artists(artist_ids: List[str], music_id: str) -> List[Dict[str, Any]]:
    """For each artistId, read ARTIST_INFO_TABLE and remove music_id from 'songs' list."""
    results = []
    for aid in set([a for a in (artist_ids or []) if a]):
        try:
            doc = artist_info_table.get_item(Key={"artistId": aid}).get("Item") or {}
            songs = doc.get("songs") or []
            if not isinstance(songs, list):
                songs = []

            if music_id in songs:
                new_songs = [m for m in songs if m != music_id]
                artist_info_table.update_item(
                    Key={"artistId": aid},
                    UpdateExpression="SET #s = :ns",
                    ExpressionAttributeNames={"#s": "songs"},
                    ExpressionAttributeValues={":ns": new_songs},
                )
                results.append({"artistId": aid, "removed": 1, "newSongCount": len(new_songs)})
            else:
                results.append({"artistId": aid, "removed": 0, "newSongCount": len(songs)})
        except Exception as e:
            results.append({"artistId": aid, "error": str(e)})
    return results

def lambda_handler(event, context):
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    if event.get("httpMethod") != "POST":
        return response(405, {"error": "Method not allowed. Use POST."})

    try:
        body = json.loads(event.get("body") or "{}")
        music_ids = body.get("musicIds")
        if not isinstance(music_ids, list) or not music_ids:
            return response(400, {"error": "musicIds (non-empty array) is required"})

        # Clean & dedupe (preserve order)
        seen, ids = set(), []
        for m in music_ids:
            s = str(m).strip()
            if s and s not in seen:
                seen.add(s)
                ids.append(s)

        per_song_results = []
        total_deleted_songs = 0
        total_deleted_index = 0
        artists_touched: Dict[str, int] = {}
        artist_updates_detail: List[Dict[str, Any]] = []

        for mid in ids:
            song = _load_song(mid)
            if not song:
                per_song_results.append({"musicId": mid, "status": "not_found"})
                continue

            genres = song.get("genres") or []
            if not isinstance(genres, list):
                genres = []
            artist_ids = song.get("artistIds") or []
            if not isinstance(artist_ids, list):
                artist_ids = []

            deleted_idx = _delete_song_and_index(mid, genres)
            total_deleted_index += deleted_idx
            total_deleted_songs += 1

            # Update artist_info.songs for all referenced artists
            updates = _remove_music_from_artists(artist_ids, mid)
            artist_updates_detail.extend(updates)
            for u in updates:
                aid = u.get("artistId")
                if aid and not u.get("error"):
                    artists_touched[aid] = artists_touched.get(aid, 0) + (u.get("removed", 0) or 0)

            per_song_results.append({
                "musicId": mid,
                "deletedSong": True,
                "deletedIndexRows": deleted_idx,
                "artistUpdates": updates,
            })

        return response(200, {
            "message": "Batch delete complete.",
            "requested": len(ids),
            "deletedSongs": total_deleted_songs,
            "deletedIndexRows": total_deleted_index,
            "artistsUpdated": artists_touched,
            "results": per_song_results,
        })

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return response(500, {"error": f"AWS error: {msg}"})
    except Exception as e:
        return response(500, {"error": str(e)})

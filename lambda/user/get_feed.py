import os, json, boto3, decimal
from boto3.dynamodb.conditions import Key
from urllib.parse import urlparse

dynamodb = boto3.resource("dynamodb")
feed_table = dynamodb.Table(os.environ["USER_FEED_TABLE"])
song_table = dynamodb.Table(os.environ["SONG_TABLE"])
S3_BUCKET  = os.environ["S3_BUCKET"]

s3c = boto3.client("s3")

def get_user_id(event):
    rc = event.get("requestContext", {})
    auth = rc.get("authorizer", {})
    if "claims" in auth:
        return auth["claims"].get("sub")
    return None

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
            "Access-Control-Allow-Methods": "GET",
        },
        "body": json.dumps(body, cls=DecimalEncoder),
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

def lambda_handler(event, context):
    print("DEBUG - incoming event:", json.dumps(event))

    user_id = get_user_id(event)
    if not user_id:
        return response(401, {"error": "Unauthorized"})

    try:
        # 1. Get all musicIds for the user
        feed_result = feed_table.query(
            KeyConditionExpression=Key("userId").eq(user_id)
        )
        feed_items = feed_result.get("Items", [])
        if not feed_items:
            return response(200, {"songs": [], "albums": []})

        music_ids = [item["musicId"] for item in feed_items]

        # 2. BatchGet all songs
        keys = [{"musicId": mid} for mid in music_ids]
        songs_resp = dynamodb.batch_get_item(
            RequestItems={song_table.name: {"Keys": keys}}
        )
        songs = songs_resp["Responses"].get(song_table.name, [])

        for song in songs:
            genres = song.get("genres")
            if isinstance(genres, list) and genres:
                song["genres"] = genres
            else:
                song["genres"] = "Unknown"

            file_url = song.get("fileUrl")
            cover_url = song.get("coverUrl")

            song["fileUrl"] = _presign_from_full_url(file_url) if file_url else None
            song["coverUrl"] = _presign_from_full_url(cover_url) or cover_url

        # 3. Group by albumId
        albums_map = {}

        for song in songs:
            album_id = song.get("albumId") or "Singles"  # ako nema albumId, ide u "Singles"

            genre = song.get("genres")
            if isinstance(genre, list):
                genres = genre
            elif genre:
                genres = [genre]
            else:
                genres = []

            if album_id not in albums_map:
                albums_map[album_id] = {
                    "albumId": album_id,
                    "songs": [],
                    "genres": set()
                }

            if song.get("musicId"):
                albums_map[album_id]["songs"].append(song["musicId"])

            albums_map[album_id]["genres"].update(genres)

        albums = []
        for album_data in albums_map.values():
            album_data["genres"] = sorted(list(album_data["genres"]))
            albums.append(album_data)

        return response(200, {"songs": songs, "albums": albums})

    except Exception as e:
        print("ERROR:", str(e))
        return response(500, {"error": str(e)})

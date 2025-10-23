import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer
from decimal import Decimal
from botocore.exceptions import ClientError
from urllib.parse import urlparse

dynamodb = boto3.resource("dynamodb")
ddb = boto3.client("dynamodb")  # client for batch_get_item
s3c = boto3.client("s3")

MUSIC_BY_GENRE_TABLE = os.environ.get("MUSIC_BY_GENRE_TABLE", "MusicByGenre")  # PK: genre, SK: musicId
SONG_TABLE = os.environ.get("SONG_TABLE", "SongTable")                          # PK: musicId
S3_BUCKET = os.environ["S3_BUCKET"]

genre_table = dynamodb.Table(MUSIC_BY_GENRE_TABLE)
song_table = dynamodb.Table(SONG_TABLE)
_deser = TypeDeserializer()

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    raise TypeError

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,GET"
        },
        "body": json.dumps(body, default=decimal_default, ensure_ascii=False)
    }

def _unmarshal(av_item: dict) -> dict:
    return {k: _deser.deserialize(v) for k, v in av_item.items()}

def _extract_key_from_url(u: str | None) -> str | None:
    if not u:
        return None
    p = urlparse(u)
    path = (p.path or "").lstrip("/")
    # Virtual-hosted or path-style
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

def _batch_get_songs_by_ids(ids):
    """BatchGet from SONG_TABLE (client API); returns dict[mid] = item with title/cover/album."""
    found = {}
    if not ids:
        return found
    CHUNK = 100
    proj_expr = "#mid,#title,#curl,#alb,#genre,#genres"
    expr_names = {
        "#mid": "musicId",
        "#title": "title",
        "#curl": "coverUrl",
        "#alb": "albumId",
        "#genre": "genre",
        "#genres": "genres",
    }

    for i in range(0, len(ids), CHUNK):
        keys = [{"musicId": {"S": mid}} for mid in ids[i:i+CHUNK]]
        req = {
            song_table.name: {
                "Keys": keys,
                "ProjectionExpression": proj_expr,
                "ExpressionAttributeNames": expr_names,
            }
        }
        for _ in range(5):
            res = ddb.batch_get_item(RequestItems=req)
            items = res.get("Responses", {}).get(song_table.name, [])
            for av in items:
                it = _unmarshal(av)
                mid = it.get("musicId")
                if mid:
                    found[mid] = it
            unp = res.get("UnprocessedKeys", {})
            if not unp or not unp.get(song_table.name, {}).get("Keys"):
                break
            req = unp
    return found

def lambda_handler(event, context):
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        genre = (event.get("queryStringParameters") or {}).get("genre")
        if not genre or not genre.strip():
            return response(400, {"error": "genre is required while filtering"})

        proj = "#g,#mid,#alb"
        names = {"#g": "genre", "#mid": "musicId", "#alb": "albumId"}
        index_items = []
        res = genre_table.query(
            KeyConditionExpression=Key("genre").eq(genre),
            ProjectionExpression=proj,
            ExpressionAttributeNames=names
        )
        index_items.extend(res.get("Items", []))
        while "LastEvaluatedKey" in res:
            res = genre_table.query(
                KeyConditionExpression=Key("genre").eq(genre),
                ProjectionExpression=proj,
                ExpressionAttributeNames=names,
                ExclusiveStartKey=res["LastEvaluatedKey"]
            )
            index_items.extend(res.get("Items", []))

        if not index_items:
            return response(200, {"albums": []})

        albums = {}
        for it in index_items:
            mid = it.get("musicId")
            alb = it.get("albumId")
            if not mid or not alb:
                continue
            if alb not in albums:
                albums[alb] = {"albumId": alb, "musicIds": [mid], "titleList": [], "coverUrl": None, "genres": set(),}
            else:
                albums[alb]["musicIds"].append(mid)

        all_ids = []
        for data in albums.values():
            all_ids.extend(data["musicIds"])
        all_ids = list(dict.fromkeys(all_ids))

        songs_by_id = _batch_get_songs_by_ids(all_ids)

        # 4) Fill cover (first non-empty among tracks)
        for alb, data in albums.items():
            cover = None
            genre_set = set()
            for mid in data["musicIds"]:
                song = songs_by_id.get(mid, {})
                if not cover and song.get("coverUrl"):
                    cover = _presign_from_full_url(song["coverUrl"]) or song["coverUrl"]

                genre_value = song.get("genres") or song.get("genre")
                if isinstance(genre_value, list):
                    genre_set.update(genre_value)
                elif genre_value:
                    genre_set.add(genre_value)

            data["coverUrl"] = cover
            data["genres"] = sorted(list(genre_set))

        return response(200, {"albums": list(albums.values())})

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return response(500, {"error": f"AWS error: {msg}"})
    except Exception as e:
        return response(500, {"error": str(e)})

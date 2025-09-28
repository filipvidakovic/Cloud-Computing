import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer
from decimal import Decimal
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
ddb = boto3.client("dynamodb")  # client for batch_get_item

MUSIC_BY_GENRE_TABLE = os.environ.get("MUSIC_BY_GENRE_TABLE", "MusicByGenre")
SONG_TABLE = os.environ.get("SONG_TABLE", "SongTable")

genre_table = dynamodb.Table(MUSIC_BY_GENRE_TABLE)  # PK: genre, SK: musicId
song_table = dynamodb.Table(SONG_TABLE)             # PK: musicId
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

def _batch_get_songs_by_ids(ids):
    """BatchGet from SONG_TABLE (client API); returns dict[mid] = item."""
    found = {}
    if not ids:
        return found
    CHUNK = 100
    proj_expr = "#mid,#title,#curl,#alb"
    expr_names = {"#mid":"musicId","#title":"title","#curl":"coverUrl","#alb":"albumId"}
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

        # 1) Query per-genre index
        proj = "#g,#mid,#alb"
        names = {"#g": "genre", "#mid": "musicId", "#alb": "albumId"}
        index_items = []
        res = genre_table.query(
            KeyConditionExpression=Key("genre").eq(genre),
            ProjectionExpression=proj,
            ExpressionAttributeNames=names
        )
        index_items += res.get("Items", [])
        while "LastEvaluatedKey" in res:
            res = genre_table.query(
                KeyConditionExpression=Key("genre").eq(genre),
                ProjectionExpression=proj,
                ExpressionAttributeNames=names,
                ExclusiveStartKey=res["LastEvaluatedKey"]
            )
            index_items += res.get("Items", [])

        if not index_items:
            return response(200, {"albums": []})

        # 2) Build albums from index first; pick ONE representative musicId per album
        albums = {}         # albumId -> { albumId, musicIds[], titleList[], coverUrl }
        reps = {}           # albumId -> representative musicId
        for it in index_items:
            mid = it.get("musicId")
            alb = it.get("albumId")
            if not mid or not alb:
                continue
            if alb not in albums:
                albums[alb] = {"albumId": alb, "musicIds": [mid], "titleList": [], "coverUrl": None}
                reps[alb] = mid  # first seen is the representative
            else:
                albums[alb]["musicIds"].append(mid)

        # 3) Fetch only representatives to get title/cover (1 read per album)
        rep_ids = list(reps.values())
        rep_songs = _batch_get_songs_by_ids(rep_ids)

        # 4) Fill titleList/coverUrl using reps; keep first non-empty cover
        for alb, rep_mid in reps.items():
            rep = rep_songs.get(rep_mid, {})
            title = rep.get("title")
            cover = rep.get("coverUrl")
            if title:
                albums[alb]["titleList"].append(title)
            if cover and not albums[alb]["coverUrl"]:
                albums[alb]["coverUrl"] = cover

        # 5) Optionally, if you want *all* titles, you could batch-get all IDs.
        #    Current approach is optimized: one song per album.

        return response(200, {"albums": list(albums.values())})

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return response(500, {"error": f"AWS error: {msg}"})
    except Exception as e:
        return response(500, {"error": str(e)})

import os, json, boto3, decimal
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
feed_table = dynamodb.Table(os.environ["USER_FEED_TABLE"])
song_table = dynamodb.Table(os.environ["SONG_TABLE"])

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
                # keep only the first one
                song["genre"] = genres[0]
            else:
                song["genre"] = "Unknown"
            # optionally drop the original genres array
            song.pop("genres", None)

        # 3. Group by albumId
        albums_map = {}
        for song in songs:
            album_id = song.get("albumId", "Unknown")
            if album_id == "Unknown":
                continue
            if album_id not in albums_map:
                albums_map[album_id] = {
                    "albumId": album_id,
                    "songs": []
                }
            albums_map[album_id]["songs"].append(song["musicId"])

        albums = list(albums_map.values())

        return response(200, {"songs": songs, "albums": albums})

    except Exception as e:
        print("ERROR:", str(e))
        return response(500, {"error": str(e)})

import json
import os
import decimal
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")

SONG_TABLE = os.environ["SONG_TABLE"]  # PK: musicId
song_table = dynamodb.Table(SONG_TABLE)

# --- JSON Decimal encoder ---
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET",
        },
        "body": json.dumps(body, cls=DecimalEncoder, ensure_ascii=False),
    }

def _norm(s):
    return str(s).strip().lower()

def lambda_handler(event, context):
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        # Expect genre and musicId as query params
        params = event.get("queryStringParameters") or {}
        genre = params.get("genre")
        music_id = params.get("musicId")

        if not genre or not music_id:
            return response(400, {"error": "genre and musicId are required"})

        # Fetch canonical song (single read)
        proj = (
            "#mid,#title,#aids,#alb,#furl,#curl,#fname,#ftype,#fsize,#created,#updated,#genres"
        )
        names = {
            "#mid": "musicId",
            "#title": "title",
            "#aids": "artistIds",
            "#alb": "albumId",
            "#furl": "fileUrl",
            "#curl": "coverUrl",
            "#fname": "fileName",
            "#ftype": "fileType",
            "#fsize": "fileSize",
            "#created": "createdAt",
            "#updated": "updatedAt",
            "#genres": "genres",
        }

        res = song_table.get_item(
            Key={"musicId": music_id},
            ProjectionExpression=proj,
            ExpressionAttributeNames=names,
            ConsistentRead=True,  # optional; keep if you want stronger read consistency
        )
        song = res.get("Item")
        if not song:
            return response(404, {"error": "Song metadata not found for given musicId"})

        # Verify genre membership via song['genres']
        song_genres = song.get("genres", [])
        if not any(_norm(g) == _norm(genre) for g in song_genres):
            return response(404, {"error": f"Song is not in genre '{genre}'"})

        # Build response straight from the canonical record
        out = {
            "musicId": song.get("musicId"),
            "genre": genre,  # as requested/verified
            "title": song.get("title"),
            "artistIds": song.get("artistIds", []),
            "albumId": song.get("albumId"),
            "fileUrl": song.get("fileUrl"),
            "coverUrl": song.get("coverUrl"),
            "fileName": song.get("fileName"),
            "fileType": song.get("fileType"),
            "fileSize": song.get("fileSize"),
            "createdAt": song.get("createdAt"),
            "updatedAt": song.get("updatedAt"),
            "genres": song_genres,  # optional to return
        }

        return response(200, out)

    except ClientError as e:
        return response(500, {"error": e.response.get("Error", {}).get("Message", str(e))})
    except Exception as e:
        return response(500, {"error": str(e)})

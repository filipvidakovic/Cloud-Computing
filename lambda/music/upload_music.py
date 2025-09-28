import json
import os
import uuid
import base64
import mimetypes
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
dynamo_client = boto3.client('dynamodb')
s3 = boto3.client('s3')

SONG_TABLE = os.environ['SONG_TABLE']
MUSIC_BY_GENRE_TABLE = os.environ['MUSIC_BY_GENRE_TABLE']
S3_BUCKET = os.environ['S3_BUCKET']
MUSIC_FOLDER = os.environ.get('MUSIC_FOLDER', 'music')
COVERS_FOLDER = os.environ.get('COVERS_FOLDER', 'covers')

# --- Table handles (resource API for simple reads later if needed) ---
song_table = dynamodb.Table(SONG_TABLE)


def response(status_code, body):
    """Uniform CORS + JSON response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,POST"
        },
        "body": json.dumps(body)
    }


def _put_object_to_s3(bucket, key, data, content_type):
    """Upload a binary object to S3."""
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    return f"https://{bucket}.s3.amazonaws.com/{key}"


def _chunked(iterable, size):
    """Yield lists of length <= size from iterable."""
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def lambda_handler(event, context):
    # Handle CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        body_raw = event.get('body', '{}')
        body = json.loads(body_raw) if isinstance(body_raw, str) else (body_raw or {})

        # Required inputs
        title = body.get('title')
        file_name = body.get('fileName')
        file_content_base64 = body.get('fileContent')
        genres = body.get('genres', [])
        artist_ids = body.get('artistIds', [])

        # Optional inputs
        album_id = body.get('albumId')
        cover_image_base64 = body.get('coverImage')

        # Validation
        if not title or not file_name or not file_content_base64 or not artist_ids or not genres:
            return response(400, {
                "error": "title, fileName, fileContent, artistIds, and genres are required"
            })
        if not isinstance(genres, list) or not all(isinstance(g, str) and g.strip() for g in genres):
            return response(400, {"error": "genres must be a non-empty list of strings"})
        if not isinstance(artist_ids, list) or not all(isinstance(a, str) and a.strip() for a in artist_ids):
            return response(400, {"error": "artistIds must be a non-empty list of strings"})

        # Upload audio to S3
        content_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
        file_bytes = base64.b64decode(file_content_base64)
        music_key = f"{MUSIC_FOLDER}/{uuid.uuid4()}-{file_name}"
        music_url = _put_object_to_s3(S3_BUCKET, music_key, file_bytes, content_type)

        # Optional cover upload
        cover_url = None
        if cover_image_base64:
            # You can derive content type if needed; many UIs send JPEG/PNG—default to JPEG
            cover_key = f"{COVERS_FOLDER}/{uuid.uuid4()}-cover.jpg"
            cover_bytes = base64.b64decode(cover_image_base64)
            cover_url = _put_object_to_s3(S3_BUCKET, cover_key, cover_bytes, "image/jpeg")

        # Build the canonical song record
        music_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        file_ext = (file_name.rsplit('.', 1)[-1] if '.' in file_name else '').lower()

        # Prepare TransactWriteItems
        actions = []

        # 1) Put full song metadata in SONG_TABLE (single source of truth)
        actions.append({
            "Put": {
                "TableName": SONG_TABLE,
                "Item": {
                    "musicId": {"S": music_id},
                    "title": {"S": title},
                    "fileName": {"S": file_name},
                    "fileType": {"S": file_ext or "unknown"},
                    "fileSize": {"N": str(len(file_bytes))},
                    "createdAt": {"S": now},
                    "updatedAt": {"S": now},
                    "artistIds": {"L": [{"S": a} for a in artist_ids]},
                    "albumId": {"S": album_id} if album_id else {"NULL": True},
                    "fileUrl": {"S": music_url},
                    "coverUrl": {"S": cover_url} if cover_url else {"NULL": True},
                    "genres": {"L": [{"S": g} for g in genres]}
                },
                "ConditionExpression": "attribute_not_exists(musicId)"
            }
        })

        # 2) For each genre, write a lightweight reference (genre, musicId, albumId?, createdAt)
        for genre in genres:
            item = {
                "genre": {"S": genre},
                "musicId": {"S": music_id},
                "createdAt": {"S": now},
            }
            if album_id:
                item["albumId"] = {"S": album_id}
            actions.append({
                "Put": {
                    "TableName": MUSIC_BY_GENRE_TABLE,
                    "Item": item
                }
            })

        # DynamoDB TransactWriteItems has a limit of 25 actions per request.
        # If we exceed it (rare—only with many genres+artists), we split into chunks.
        # We always write the SONG_TABLE put first to ensure ID existence.
        if len(actions) <= 25:
            dynamo_client.transact_write_items(TransactItems=actions)
        else:
            # Write the first action (Put song) alone
            dynamo_client.transact_write_items(TransactItems=[actions[0]])
            # Then chunk the rest
            for batch in _chunked(actions[1:], 25):
                dynamo_client.transact_write_items(TransactItems=batch)

        # Success
        return response(201, {
            "message": "Music content uploaded successfully (normalized)",
            "musicId": music_id,
            "title": title,
            "genres": genres,
            "albumId": album_id,
            "fileUrl": music_url,
            "coverUrl": cover_url
        })

    except ClientError as e:
        # Surface AWS errors
        return response(500, {"error": str(e)})
    except Exception as e:
        # Catch-all for unexpected errors
        return response(500, {"error": str(e)})

import json
import boto3
import os
import uuid
from botocore.exceptions import ClientError
from datetime import datetime
import base64

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

MUSIC_TABLE = os.environ['MUSIC_TABLE']
S3_BUCKET = os.environ['S3_BUCKET']
MUSIC_FOLDER = os.environ.get('MUSIC_FOLDER', 'music')
COVERS_FOLDER = os.environ.get('COVERS_FOLDER', 'covers')

table = dynamodb.Table(MUSIC_TABLE)

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,POST"
        },
        "body": json.dumps(body)
    }

def lambda_handler(event, context):
    # Handle CORS preflight request
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        body = json.loads(event.get('body', '{}'))

        # Required fields
        title = body.get('title')
        file_name = body.get('fileName')
        file_content_base64 = body.get('fileContent')
        genres = body.get('genres', [])
        artist_ids = body.get('artistIds', [])
        album_id = body.get('albumId')
        cover_image_base64 = body.get('coverImage')

        if not title or not file_name or not file_content_base64 or not artist_ids or not genres:
            return response(400, {
                "error": "title, fileName, fileContent, artistIds, and genres are required"
            })

        # Decode and upload audio file
        file_bytes = base64.b64decode(file_content_base64)
        music_key = f"{MUSIC_FOLDER}/{uuid.uuid4()}-{file_name}"
        s3.put_object(Bucket=S3_BUCKET, Key=music_key, Body=file_bytes)
        music_url = f"https://{S3_BUCKET}.s3.amazonaws.com/{music_key}"

        # Optional cover image
        cover_url = None
        if cover_image_base64:
            cover_key = f"{COVERS_FOLDER}/{uuid.uuid4()}-cover.jpg"
            s3.put_object(Bucket=S3_BUCKET, Key=cover_key, Body=base64.b64decode(cover_image_base64))
            cover_url = f"https://{S3_BUCKET}.s3.amazonaws.com/{cover_key}"

        # Save metadata to DynamoDB for each genre
        music_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        for genre in genres:
            music_item = {
                "genre": genre,
                "musicId": music_id,
                "title": title,
                "fileName": file_name,
                "fileType": file_name.split('.')[-1],
                "fileSize": len(file_bytes),
                "createdAt": now,
                "updatedAt": now,
                "artistIds": artist_ids,
                "albumId": album_id,
                "fileUrl": music_url,
                "coverUrl": cover_url
            }
            table.put_item(Item=music_item)

        return response(201, {
            "message": "Music content uploaded successfully",
            "musicId": music_id,
            "title": title,
            "genres": genres,
            "fileUrl": music_url,
            "coverUrl": cover_url
        })

    except ClientError as e:
        return response(500, {"error": str(e)})
    except Exception as e:
        return response(500, {"error": str(e)})

import json
import boto3
import os
import uuid
import base64
from datetime import datetime
from botocore.exceptions import ClientError
import decimal

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

MUSIC_TABLE = os.environ['MUSIC_TABLE']
S3_BUCKET = os.environ['S3_BUCKET']
MUSIC_FOLDER = os.environ.get('MUSIC_FOLDER', 'music')
COVERS_FOLDER = os.environ.get('COVERS_FOLDER', 'covers')

table = dynamodb.Table(MUSIC_TABLE)

# Decimal encoder for JSON
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
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,POST,PUT"
        },
        "body": json.dumps(body, cls=DecimalEncoder)
    }

def lambda_handler(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        body = json.loads(event.get('body', '{}'))

        music_id = body.get('musicId')
        title = body.get('title')
        file_name = body.get('fileName')
        file_content_base64 = body.get('fileContent')
        cover_image_base64 = body.get('coverImage')

        if not music_id:
            return response(400, {"error": "musicId is required"})

        # Fetch all existing records with this musicId
        scan_result = table.scan(
            FilterExpression="musicId = :m",
            ExpressionAttributeValues={":m": music_id}
        )
        existing_items = scan_result.get("Items", [])

        if not existing_items:
            return response(404, {"error": "No music entry found with given musicId"})

        now = datetime.utcnow().isoformat()
        music_url = None
        cover_url = None

        # Upload new audio file if provided
        if file_content_base64 and file_name:
            file_bytes = base64.b64decode(file_content_base64)
            music_key = f"{MUSIC_FOLDER}/{uuid.uuid4()}-{file_name}"
            s3.put_object(Bucket=S3_BUCKET, Key=music_key, Body=file_bytes)
            music_url = f"https://{S3_BUCKET}.s3.amazonaws.com/{music_key}"

        # Upload new cover image if provided
        if cover_image_base64:
            cover_key = f"{COVERS_FOLDER}/{uuid.uuid4()}-cover.jpg"
            s3.put_object(Bucket=S3_BUCKET, Key=cover_key, Body=base64.b64decode(cover_image_base64))
            cover_url = f"https://{S3_BUCKET}.s3.amazonaws.com/{cover_key}"

        # Update each genre entry for the musicId
        for item in existing_items:
            genre = item["genre"]

            updated_item = {
                "genre": genre,
                "musicId": music_id,
                "title": title if title is not None else item["title"],
                "fileName": file_name if file_name is not None else item["fileName"],
                "fileType": (file_name or item["fileName"]).split('.')[-1],
                "fileSize": len(base64.b64decode(file_content_base64)) if file_content_base64 else item.get("fileSize", 0),
                "fileUrl": music_url if music_url is not None else item["fileUrl"],
                "coverUrl": cover_url if cover_url is not None else item.get("coverUrl"),
                "albumId": item.get("albumId"),
                "artistIds": item.get("artistIds", []),
                "createdAt": item.get("createdAt", now),
                "updatedAt": now,
            }

            table.put_item(Item=updated_item)

        return response(200, {
            "message": "Music updated successfully",
            "musicId": music_id,
            "fileUrl": music_url,
            "coverUrl": cover_url
        })

    except ClientError as e:
        return response(500, {"error": str(e)})
    except Exception as e:
        return response(500, {"error": str(e)})

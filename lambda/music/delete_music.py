import json
import boto3
import os
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

MUSIC_TABLE = os.environ['MUSIC_TABLE']
S3_BUCKET = os.environ['S3_BUCKET']

table = dynamodb.Table(MUSIC_TABLE)

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,DELETE,PUT"
        },
        "body": json.dumps(body)
    }

def lambda_handler(event, context):
    # Handle CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        # Get musicId from query parameters
        params = event.get("queryStringParameters") or {}
        music_id = params.get("musicId")

        if not music_id:
            return response(400, {"error": "Query parameter 'musicId' is required"})

        # Scan for items matching musicId
        scan_result = table.scan(
            FilterExpression="musicId = :mid",
            ExpressionAttributeValues={":mid": music_id}
        )
        items = scan_result.get("Items", [])

        if not items:
            return response(404, {"error": f"No music entries found with musicId: {music_id}"})

        deleted_files = []
        deleted_covers = []

        # Delete each entry and associated files
        for item in items:
            genre = item["genre"]
            table.delete_item(Key={"genre": genre, "musicId": music_id})

            # Delete music file
            if "fileUrl" in item:
                try:
                    key = item["fileUrl"].split(f"{S3_BUCKET}.s3.amazonaws.com/")[1]
                    s3.delete_object(Bucket=S3_BUCKET, Key=key)
                    deleted_files.append(key)
                except Exception as e:
                    print(f"Could not delete music file: {e}")

            # Delete cover image
            if "coverUrl" in item:
                try:
                    key = item["coverUrl"].split(f"{S3_BUCKET}.s3.amazonaws.com/")[1]
                    s3.delete_object(Bucket=S3_BUCKET, Key=key)
                    deleted_covers.append(key)
                except Exception as e:
                    print(f"Could not delete cover image: {e}")

        return response(200, {
            "message": f"Deleted {len(items)} record(s) with musicId: {music_id}",
            "deletedS3Files": deleted_files,
            "deletedCoverImages": deleted_covers
        })

    except ClientError as e:
        return response(500, {"error": str(e)})
    except Exception as e:
        return response(500, {"error": str(e)})

import json
import boto3
import os
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

MUSIC_TABLE = os.environ['MUSIC_TABLE']
S3_BUCKET = os.environ['S3_BUCKET']

table = dynamodb.Table(MUSIC_TABLE)

def lambda_handler(event, context):
    try:
        # Get musicId from query params
        params = event.get("queryStringParameters") or {}
        music_id = params.get("musicId")

        if not music_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Query parameter 'musicId' is required"})
            }

        # Scan the table for items with that musicId
        response = table.scan(
            FilterExpression="musicId = :mid",
            ExpressionAttributeValues={":mid": music_id}
        )
        items = response.get("Items", [])

        if not items:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "No music entries found with that musicId"})
            }

        # Delete each found item
        for item in items:
            genre = item["genre"]
            table.delete_item(Key={"genre": genre, "musicId": music_id})

            # Optionally delete audio and cover from S3
            if "fileUrl" in item:
                try:
                    key = item["fileUrl"].split(f"{S3_BUCKET}.s3.amazonaws.com/")[1]
                    s3.delete_object(Bucket=S3_BUCKET, Key=key)
                except Exception as e:
                    print(f"Could not delete music file: {e}")

            if "coverUrl" in item:
                try:
                    key = item["coverUrl"].split(f"{S3_BUCKET}.s3.amazonaws.com/")[1]
                    s3.delete_object(Bucket=S3_BUCKET, Key=key)
                except Exception as e:
                    print(f"Could not delete cover image: {e}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Deleted {len(items)} music record(s) with musicId: {music_id}"
            })
        }

    except ClientError as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

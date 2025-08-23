import json
import boto3
import os
from botocore.exceptions import ClientError
from urllib.parse import urlparse

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

MUSIC_TABLE = os.environ['MUSIC_TABLE']
S3_BUCKET = os.environ['S3_BUCKET']
table = dynamodb.Table(MUSIC_TABLE)

def delete_s3_object(url):
    try:
        key = urlparse(url).path.lstrip("/")
        s3.delete_object(Bucket=S3_BUCKET, Key=key)
    except Exception as e:
        print(f"Failed to delete {url}: {e}")

def lambda_handler(event, context):
    try:
        artist_id = event.get("pathParameters", {}).get("artistId")
        if not artist_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing artistId"})
            }

        deleted = []
        scan_kwargs = {
            "FilterExpression": "contains(artistIds, :artistId)",
            "ExpressionAttributeValues": {":artistId": artist_id}
        }

        last_evaluated_key = None
        while True:
            if last_evaluated_key:
                scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

            response = table.scan(**scan_kwargs)
            items = response.get("Items", [])

            for item in items:
                genre = item["genre"]
                music_id = item["musicId"]
                table.delete_item(Key={"genre": genre, "musicId": music_id})
                deleted.append(music_id)

                if item.get("fileUrl"):
                    delete_s3_object(item["fileUrl"])
                if item.get("coverUrl"):
                    delete_s3_object(item["coverUrl"])

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Deleted {len(deleted)} songs for artist {artist_id}",
                "deletedSongs": deleted
            })
        }

    except ClientError as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

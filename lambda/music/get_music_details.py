import json
import boto3
import os
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['MUSIC_TABLE'])

def lambda_handler(event, context):
    try:
        # Expect genre and title as query params
        params = event.get('queryStringParameters') or {}
        genre = params.get('genre')
        musicId = params.get('musicId')

        if not genre or not musicId:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "genre and musicId are required"})
            }

        # Get the item
        response = table.get_item(Key={"genre": genre, "musicId": musicId})
        item = response.get('Item')

        if not item:
            return {"statusCode": 404, "body": json.dumps({"error": "Music not found"})}

        # Optionally: remove internal fields, add artist/album info if you have a lookup

        return {
            "statusCode": 200,
            "body": json.dumps({
                "musicId": item["musicId"],
                "title": item["title"],
                "genre": item["genre"],
                "artistIds": item["artistIds"],
                "albumId": item.get("albumId"),
                "fileUrl": item["fileUrl"],
                "coverUrl": item.get("coverUrl"),
                "fileName": item["fileName"],
                "fileType": item["fileType"],
                "fileSize": item["fileSize"],
                "createdAt": item["createdAt"]
            })
        }

    except ClientError as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

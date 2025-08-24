import decimal
import json
import boto3
import os
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['MUSIC_TABLE'])

# Custom encoder for Decimal values from DynamoDB
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        "body": json.dumps(body, cls=DecimalEncoder)
    }

def lambda_handler(event, context):
    # Handle CORS preflight request
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        # Expect genre and musicId as query params
        params = event.get('queryStringParameters') or {}
        genre = params.get('genre')
        musicId = params.get('musicId')

        if not genre or not musicId:
            return response(400, {"error": "genre and musicId are required"})

        # Get the item
        result = table.get_item(Key={"genre": genre, "musicId": musicId})
        item = result.get('Item')

        if not item:
            return response(404, {"error": "Music not found"})

        return response(200, {
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

    except ClientError as e:
        return response(500, {"error": str(e)})

    except Exception as e:
        return response(500, {"error": str(e)})

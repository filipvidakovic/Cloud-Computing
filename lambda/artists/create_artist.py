import json
import boto3
import os
import uuid
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
ARTISTS_TABLE = os.environ['ARTISTS_TABLE']  # Pass table name via Lambda environment variables
table = dynamodb.Table(ARTISTS_TABLE)

def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', '{}'))
        name = body.get('name')
        lastname = body.get('lastname')
        age = body.get('age')
        bio = body.get('bio', '')
        genres = body.get('genres', [])

        if not name or not lastname or not age:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "name, lastname and age are required"})
            }

        try:
            age = int(age)
        except ValueError:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "age must be a number"})
            }

        artist_id = str(uuid.uuid4())

        artist = {
            "artistId": artist_id,
            "name": name,
            "lastname": lastname,
            "age": int(age),
            "bio": bio,
            "genres": genres
        }

        table.put_item(Item=artist)

        return {
            "statusCode": 201,
            "body": json.dumps({
                "message": "Artist created successfully",
                "artist": artist
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
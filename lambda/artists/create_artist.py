import json
import boto3
import os
import uuid
from botocore.exceptions import ClientError

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb")
ARTISTS_TABLE = os.environ["ARTISTS_TABLE"]
table = dynamodb.Table(ARTISTS_TABLE)

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,DELETE"
        },
        "body": json.dumps(body)
    }

def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))

        name = body.get("name")
        lastname = body.get("lastname")
        age = body.get("age")
        bio = body.get("bio", "")
        genres = body.get("genres", [])

        if not name or not lastname or not age or not genres:
            return response(400, {"error": "name, lastname, age and at least one genre are required"})

        try:
            age = int(age)
        except ValueError:
            return response(400, {"error": "age must be a number"})

        artist_id = str(uuid.uuid4())

        # create one item per genre
        with table.batch_writer() as batch:
            for genre in genres:
                item = {
                    "artistId": artist_id,     # Partition key
                    "genre": genre,            # Sort key
                    "name": name,
                    "lastname": lastname,
                    "age": age,
                    "bio": bio
                }
                batch.put_item(Item=item)

        return response(201, {
            "message": "Artist created successfully",
            "artist": {
                "artistId": artist_id,
                "name": name,
                "lastname": lastname,
                "age": age,
                "bio": bio,
                "genres": genres
            }
        })

    except ClientError as e:
        return response(500, {"error": str(e)})
    except Exception as e:
        return response(500, {"error": str(e)})

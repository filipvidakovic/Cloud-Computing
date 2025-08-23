import json
import boto3
import os
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
ARTISTS_TABLE = os.environ['ARTISTS_TABLE']
table = dynamodb.Table(ARTISTS_TABLE)

lambda_client = boto3.client("lambda")
DELETE_SONGS_FUNCTION = os.environ["DELETE_SONGS_FUNCTION"]

def lambda_handler(event, context):
    try:
        # Extract artistId from path parameters or body
        path_params = event.get("pathParameters") or {}
        body = json.loads(event.get("body", "{}"))

        artist_id = path_params.get("artistId") or body.get("artistId")

        if not artist_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "artistId is required"})
            }

        # Check if the artist exists
        existing = table.get_item(Key={"artistId": artist_id})
        if "Item" not in existing:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "Artist not found"})
            }

        # Perform deletion
        table.delete_item(Key={"artistId": artist_id})

        # invoke lambda for deletion of songs by the artist
        lambda_client.invoke(
            FunctionName=DELETE_SONGS_FUNCTION,
            InvocationType="Event",
            Payload=json.dumps({"pathParameters": {"artistId": artist_id}}).encode("utf-8")
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Artist {artist_id} deleted successfully."})
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

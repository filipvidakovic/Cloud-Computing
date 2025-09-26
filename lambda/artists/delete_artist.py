import json
import boto3
import os
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")

ARTISTS_TABLE = os.environ["ARTISTS_TABLE"]
ARTIST_INFO_TABLE = os.environ["ARTIST_INFO_TABLE"]
DELETE_SONGS_FUNCTION = os.environ["DELETE_SONGS_FUNCTION"]

artist_table = dynamodb.Table(ARTISTS_TABLE)
info_table = dynamodb.Table(ARTIST_INFO_TABLE)

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,DELETE"
        },
        "body": json.dumps(body)
    }

def _query_all_artist_rows(artist_id: str):
    items, last_evaluated_key = [], None
    while True:
        query_kwargs = {"KeyConditionExpression": Key("artistId").eq(artist_id)}
        if last_evaluated_key:
            query_kwargs["ExclusiveStartKey"] = last_evaluated_key

        resp = artist_table.query(**query_kwargs)
        items.extend(resp.get("Items", []))
        last_evaluated_key = resp.get("LastEvaluatedKey")

        if not last_evaluated_key:
            break
    return items

def lambda_handler(event, context):
    try:
        path_params = event.get("pathParameters") or {}
        body = json.loads(event.get("body", "{}"))

        artist_id = path_params.get("artistId") or body.get("artistId")
        if not artist_id:
            return response(400, {"error": "artistId is required"})

        profile = info_table.get_item(Key={"artistId": artist_id}).get("Item")
        if not profile:
            return response(404, {"error": "Artist not found"})

        # delete all artist rows from ArtistTable
        items = _query_all_artist_rows(artist_id)
        with artist_table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={"artistId": item["artistId"], "genre": item["genre"]})

        # deleet artist profile from ArtistInfoTable
        info_table.delete_item(Key={"artistId": artist_id})

        # IMPLEMENT delete songs for this artist


        return response(200, {
            "message": f"Artist {artist_id} deleted successfully. Songs cleanup triggered."
        })

    except ClientError as e:
        return response(500, {"error": str(e)})
    except Exception as e:
        return response(500, {"error": str(e)})

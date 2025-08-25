import json
import boto3
import os
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
ARTISTS_TABLE = os.environ["ARTISTS_TABLE"]
table = dynamodb.Table(ARTISTS_TABLE)

lambda_client = boto3.client("lambda")
DELETE_SONGS_FUNCTION = os.environ["DELETE_SONGS_FUNCTION"]

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
def _query_all_artist_rows(artist_id: str):
    items, last_evaluated_key = [], None

    while True:
        query_kwargs = {"KeyConditionExpression": Key("artistId").eq(artist_id)}
        if last_evaluated_key:
            query_kwargs["ExclusiveStartKey"] = last_evaluated_key

        resp = table.query(**query_kwargs)
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

        items = _query_all_artist_rows(artist_id)

        if not items:
            return response(404, {"error": "Artist not found"})

        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={"artistId": item["artistId"], "genre": item["genre"]})

        try:
            lambda_client.invoke(
                FunctionName=DELETE_SONGS_FUNCTION,
                InvocationType="Event",
                Payload=json.dumps({"pathParameters": {"artistId": artist_id}}).encode("utf-8"),
            )
        except Exception as invoke_err:
            return response(200, {
                "message": f"Artist {artist_id} deleted. Song cleanup invoke failed.",
                "warning": str(invoke_err)
            })

        return response(200, {"message": f"Artist {artist_id} deleted successfully. Songs cleanup triggered."})

    except ClientError as e:
        return response(500, {"error": str(e)})
    except Exception as e:
        return response(500, {"error": str(e)})

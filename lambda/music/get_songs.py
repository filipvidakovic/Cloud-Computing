import os
import json
import boto3
import decimal

dynamodb = boto3.resource("dynamodb")
song_table = dynamodb.Table(os.environ["SONG_TABLE"])


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


def response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET"
        },
        "body": json.dumps(body, cls=DecimalEncoder)
    }


def lambda_handler(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        params = event.get("queryStringParameters") or {}
        limit = int(params.get("limit", 50))
        last_key_raw = params.get("lastKey")

        scan_kwargs = {"Limit": limit}

        if last_key_raw:
            try:
                last_key = json.loads(last_key_raw)
                if not isinstance(last_key, dict):
                    raise ValueError("lastKey must be a dict")
                scan_kwargs["ExclusiveStartKey"] = last_key
            except Exception as e:
                return response(400, {"error": f"Invalid lastKey format: {str(e)}"})

        result = song_table.scan(**scan_kwargs)
        items = result.get("Items", [])
        last_evaluated_key = result.get("LastEvaluatedKey")

        return response(200, {
            "songs": items,
            "lastKey": json.dumps(last_evaluated_key, cls=DecimalEncoder) if last_evaluated_key else None
        })

    except Exception as e:
        return response(500, {"error": str(e)})

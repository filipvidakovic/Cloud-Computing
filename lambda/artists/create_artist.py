import json, os, uuid, boto3, time
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
client = boto3.client("dynamodb")  # needed for transact_write_items
ARTISTS_TABLE = os.environ["ARTISTS_TABLE"]
ARTIST_INFO_TABLE = os.environ["ARTIST_INFO_TABLE"]

def response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,PUT,DELETE"
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

        if not name or not lastname or age is None or not genres:
            return response(400, {"error": "name, lastname, age and at least one genre are required"})

        try:
            age = int(age)
        except ValueError:
            return response(400, {"error": "age must be a number"})

        artist_id = str(uuid.uuid4())

        transact_items = []

        # 1) profile in ArtistInfoTable
        transact_items.append({
            "Put": {
                "TableName": ARTIST_INFO_TABLE,
                "Item": {
                    "artistId": {"S": artist_id},
                    "name": {"S": name},
                    "lastname": {"S": lastname},
                    "age": {"N": str(age)},
                    "bio": {"S": bio},
                    "genres": {"L": [{"S": g} for g in genres]}
                },
                # prevent accidental overwrite of existing ID
                "ConditionExpression": "attribute_not_exists(artistId)"
            }
        })

        # (artist, genre) in ArtistTable
        for g in genres:
            transact_items.append({
                "Put": {
                    "TableName": ARTISTS_TABLE,
                    "Item": {
                        "artistId": {"S": artist_id},
                        "genre": {"S": g}
                    }
                }
            })

        # commit everything together
        client.transact_write_items(TransactItems=transact_items)

        return response(201, {
            "message": "Artist created",
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

import json
import os
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal  # <-- add

dynamodb = boto3.resource("dynamodb")
dynamo_client = boto3.client("dynamodb")

ARTISTS_TABLE = os.environ["ARTISTS_TABLE"]        # PK: artistId, SK: genre
ARTIST_INFO_TABLE = os.environ["ARTIST_INFO_TABLE"]  # PK: artistId

artist_table = dynamodb.Table(ARTISTS_TABLE)
info_table = dynamodb.Table(ARTIST_INFO_TABLE)

# ---- NEW: recursively convert Decimal to int/float so json.dumps works
def _convert_decimals(obj):
    if isinstance(obj, list):
        return [_convert_decals(i) for i in obj]  # typo fix below
    elif isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        # int if it has no fractional part, else float
        return int(obj) if obj % 1 == 0 else float(obj)
    else:
        return obj

# (typo fix)
def _convert_decals(x):  # helper to keep comprehension compact
    return _convert_decimals(x)

def response(status, body):
    # Ensure body is JSON-serializable even if it contains Decimals
    clean = _convert_decimals(body)
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,PUT,DELETE"
        },
        "body": json.dumps(clean)
    }

def _load_profile(artist_id: str):
    item = info_table.get_item(Key={"artistId": artist_id}).get("Item")
    return item  # keep raw here; we'll convert right before returning

def _current_genres(artist_id: str):
    res = artist_table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("artistId").eq(artist_id),
        ProjectionExpression="#g",
        ExpressionAttributeNames={"#g": "genre"}
    )
    return [it["genre"] for it in res.get("Items", [])]

def _chunk(iterable, size):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf

def lambda_handler(event, context):
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        method = event.get("httpMethod", "PUT")
        if method not in ("PUT", "POST"):  # allow POST if you prefer
            return response(405, {"error": "Use PUT"})

        path_params = event.get("pathParameters") or {}
        body_raw = event.get("body", "{}")
        body = json.loads(body_raw) if isinstance(body_raw, str) else (body_raw or {})

        artist_id = path_params.get("artistId") or body.get("artistId")
        if not artist_id:
            return response(400, {"error": "artistId is required"})

        # Fetch existing profile
        profile = _load_profile(artist_id)
        if not profile:
            return response(404, {"error": "Artist not found"})

        # Fields to update (partial allowed)
        name = body.get("name")
        lastname = body.get("lastname")
        age = body.get("age")  # may be None / omitted
        bio = body.get("bio")
        genres = body.get("genres")  # when provided, must be a list of strings

        updates = {}
        if name is not None:
            if not isinstance(name, str) or not name.strip():
                return response(400, {"error": "name must be a non-empty string"})
            updates["name"] = name.strip()

        if lastname is not None:
            if not isinstance(lastname, str) or not lastname.strip():
                return response(400, {"error": "lastname must be a non-empty string"})
            updates["lastname"] = lastname.strip()

        if age is not None:
            try:
                age = int(age)
                updates["age"] = age
            except Exception:
                return response(400, {"error": "age must be a number"})

        if bio is not None:
            if not isinstance(bio, str):
                return response(400, {"error": "bio must be a string"})
            updates["bio"] = bio

        genres_provided = False
        new_genres = None
        if genres is not None:
            genres_provided = True
            if (not isinstance(genres, list)) or (not all(isinstance(g, str) and g.strip() for g in genres)):
                return response(400, {"error": "genres must be a list of non-empty strings"})
            # normalize, dedupe, keep order
            seen = set()
            new_genres = []
            for g in genres:
                g2 = g.strip()
                if g2 not in seen:
                    seen.add(g2)
                    new_genres.append(g2)
            updates["genres"] = new_genres

        if not updates and not genres_provided:
            # nothing to change
            # Convert Decimals in profile before returning
            return response(200, {"message": "No changes", "artist": _convert_decimals(profile)})

        # Compute current genres if genres update is requested
        current_genres = profile.get("genres", [])
        if genres_provided and not current_genres:
            current_genres = _current_genres(artist_id)

        # Build TransactWrite for updating ArtistInfo + syncing ARTISTS_TABLE rows (for genres)
        transact = []

        # Update expression for ArtistInfoTable
        if updates:
            set_parts = []
            expr_attr_vals = {}
            expr_attr_names = {}

            def an(name):  # attribute name placeholder
                return f"#{name}"
            def av(name):  # attribute value placeholder
                return f":{name}"

            if "name" in updates:
                set_parts.append(f"{an('name')} = {av('name')}")
                expr_attr_vals[av("name")] = {"S": updates["name"]}
                expr_attr_names[an("name")] = "name"

            if "lastname" in updates:
                set_parts.append(f"{an('lastname')} = {av('lastname')}")
                expr_attr_vals[av('lastname')] = {"S": updates["lastname"]}
                expr_attr_names[an('lastname')] = "lastname"

            if "age" in updates:
                set_parts.append(f"{an('age')} = {av('age')}")
                expr_attr_vals[av("age")] = {"N": str(updates["age"])}
                expr_attr_names[an("age")] = "age"

            if "bio" in updates:
                set_parts.append(f"{an('bio')} = {av('bio')}")
                expr_attr_vals[av("bio")] = {"S": updates["bio"]}
                expr_attr_names[an("bio")] = "bio"

            if genres_provided:
                set_parts.append(f"{an('genres')} = {av('genres')}")
                expr_attr_vals[av("genres")] = {"L": [{"S": g} for g in new_genres]}
                expr_attr_names[an("genres")] = "genres"

            update_expr = "SET " + ", ".join(set_parts) if set_parts else None

            if update_expr:
                transact.append({
                    "Update": {
                        "TableName": ARTIST_INFO_TABLE,
                        "Key": {"artistId": {"S": artist_id}},
                        "UpdateExpression": update_expr,
                        "ExpressionAttributeNames": expr_attr_names or None,
                        "ExpressionAttributeValues": expr_attr_vals or None,
                        "ReturnValuesOnConditionCheckFailure": "NONE",
                        "ConditionExpression": "attribute_exists(artistId)"
                    }
                })

        if genres_provided:
            current_set = set(current_genres)
            new_set = set(new_genres)

            to_add = sorted(list(new_set - current_set))
            to_remove = sorted(list(current_set - new_set))

            for g in to_add:
                transact.append({
                    "Put": {
                        "TableName": ARTISTS_TABLE,
                        "Item": {"artistId": {"S": artist_id}, "genre": {"S": g}},
                        "ReturnValuesOnConditionCheckFailure": "NONE",
                    }
                })
            for g in to_remove:
                transact.append({
                    "Delete": {
                        "TableName": ARTISTS_TABLE,
                        "Key": {"artistId": {"S": artist_id}, "genre": {"S": g}},
                        "ReturnValuesOnConditionCheckFailure": "NONE",
                    }
                })

        if not transact:
            updated = _load_profile(artist_id)
            return response(200, {"message": "No changes", "artist": _convert_decimals(updated)})

        if len(transact) <= 25:
            dynamo_client.transact_write_items(TransactItems=transact)
        else:
            dynamo_client.transact_write_items(TransactItems=[transact[0]])
            def chunks(seq, n):
                for i in range(0, len(seq), n):
                    yield seq[i:i+n]
            for batch in chunks(transact[1:], 25):
                dynamo_client.transact_write_items(TransactItems=batch)

        updated = _load_profile(artist_id)
        return response(200, {"message": "Artist updated", "artist": _convert_decimals(updated)})

    except ClientError as e:
        return response(500, {"error": str(e)})
    except Exception as e:
        return response(500, {"error": str(e)})

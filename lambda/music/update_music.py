import json
import os
import uuid
import base64
import mimetypes
import decimal
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

# --- AWS clients/resources ---
dynamodb = boto3.resource('dynamodb')
dynamo_client = boto3.client('dynamodb')
s3 = boto3.client('s3')

# --- Env vars ---
SONG_TABLE = os.environ['SONG_TABLE']                 # PK: musicId
MUSIC_BY_GENRE_TABLE = os.environ['MUSIC_BY_GENRE_TABLE']  # PK: genre, SK: musicId
S3_BUCKET = os.environ['S3_BUCKET']
MUSIC_FOLDER = os.environ.get('MUSIC_FOLDER', 'music')
COVERS_FOLDER = os.environ.get('COVERS_FOLDER', 'covers')

song_table = dynamodb.Table(SONG_TABLE)
genre_table = dynamodb.Table(MUSIC_BY_GENRE_TABLE)


# Decimal encoder so JSON dumps can handle DynamoDB Decimals
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,POST,PUT"
        },
        "body": json.dumps(body, cls=DecimalEncoder)
    }


def _put_object_to_s3(bucket: str, key: str, data: bytes, content_type: str) -> str:
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    return f"https://{bucket}.s3.amazonaws.com/{key}"


def _chunked(iterable, size):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _marshal_expr_attr_vals(python_vals: dict) -> dict:
    """
    Convert ExpressionAttributeValues from native Python types to the low-level
    AttributeValue format required by transact_write_items.
    We only pass scalars/list[str]/numbers here, so handle a few types.
    """
    out = {}
    for k, v in python_vals.items():
        if isinstance(v, str):
            out[k] = {"S": v}
        elif isinstance(v, (int, float, decimal.Decimal)):
            out[k] = {"N": str(v)}
        elif isinstance(v, list):
            out[k] = {"L": [{"S": s} for s in v]}
        else:
            out[k] = {"S": str(v)}
    return out


def lambda_handler(event, context):
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        raw = event.get('body', '{}')
        body = json.loads(raw) if isinstance(raw, str) else (raw or {})

        music_id = body.get('musicId')
        if not music_id:
            return response(400, {"error": "musicId is required"})

        # Fetch current song (we’ll use its current genres & albumId)
        current = song_table.get_item(Key={"musicId": music_id}).get("Item")
        if not current:
            return response(404, {"error": "Song not found for given musicId"})

        current_genres = set(current.get("genres", []) or [])
        current_album_id = current.get("albumId")  # may be None

        # Updatable fields
        title = body.get('title')
        artist_ids = body.get('artistIds')
        file_name = body.get('fileName')
        file_content_b64 = body.get('fileContent')
        cover_image_b64 = body.get('coverImage')
        new_genres = body.get('genres')
        album_id_in = body.get('albumId') if 'albumId' in body else None
        album_change_requested = ('albumId' in body)  # True even if null (clear)

        # Build the SONG_TABLE update
        now = datetime.utcnow().isoformat()
        expr_attr_names = {"#updatedAt": "updatedAt"}
        expr_attr_vals = {":updatedAt": now}
        set_clauses = ["#updatedAt = :updatedAt"]
        remove_clauses = []

        # Audio update
        if file_content_b64:
            if not file_name:
                return response(400, {"error": "fileName is required when updating fileContent"})
            file_bytes = base64.b64decode(file_content_b64)
            content_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
            music_key = f"{MUSIC_FOLDER}/{uuid.uuid4()}-{file_name}"
            file_url = _put_object_to_s3(S3_BUCKET, music_key, file_bytes, content_type)
            file_ext = (file_name.rsplit('.', 1)[-1] if '.' in file_name else '').lower()

            expr_attr_names.update({"#fileUrl": "fileUrl", "#fileType": "fileType", "#fileSize": "fileSize"})
            expr_attr_vals.update({
                ":fileUrl": file_url,
                ":fileType": file_ext or "unknown",
                ":fileSize": len(file_bytes)
            })
            set_clauses += ["#fileUrl = :fileUrl", "#fileType = :fileType", "#fileSize = :fileSize"]

        # Cover update
        if cover_image_b64:
            cover_key = f"{COVERS_FOLDER}/{uuid.uuid4()}-cover.jpg"
            cover_bytes = base64.b64decode(cover_image_b64)
            cover_url = _put_object_to_s3(S3_BUCKET, cover_key, cover_bytes, "image/jpeg")

            expr_attr_names["#coverUrl"] = "coverUrl"
            expr_attr_vals[":coverUrl"] = cover_url
            set_clauses.append("#coverUrl = :coverUrl")

        # Title
        if title is not None:
            expr_attr_names["#title"] = "title"
            expr_attr_vals[":title"] = title
            set_clauses.append("#title = :title")

        # fileName metadata
        if file_name is not None:
            expr_attr_names["#fileName"] = "fileName"
            expr_attr_vals[":fileName"] = file_name
            set_clauses.append("#fileName = :fileName")

        # artistIds (replace)
        if artist_ids is not None:
            if not isinstance(artist_ids, list) or not all(isinstance(a, str) and a.strip() for a in artist_ids):
                return response(400, {"error": "artistIds must be a list of non-empty strings"})
            expr_attr_names["#artistIds"] = "artistIds"
            expr_attr_vals[":artistIds"] = artist_ids
            set_clauses.append("#artistIds = :artistIds")

        # genres (replace in SONG_TABLE)
        genres_delta = None
        desired_genres = None
        if new_genres is not None:
            if not isinstance(new_genres, list) or not all(isinstance(g, str) and g.strip() for g in new_genres):
                return response(400, {"error": "genres must be a list of non-empty strings"})
            desired_genres = set(new_genres)
            expr_attr_names["#genres"] = "genres"
            expr_attr_vals[":genres"] = list(desired_genres)
            set_clauses.append("#genres = :genres")

        # albumId (update or clear if explicitly sent)
        new_album_id_effective = current_album_id
        if album_change_requested:
            expr_attr_names["#albumId"] = "albumId"
            if album_id_in is None:
                # clear albumId
                remove_clauses.append("#albumId")
                new_album_id_effective = None
            else:
                expr_attr_vals[":albumId"] = album_id_in
                set_clauses.append("#albumId = :albumId")
                new_album_id_effective = album_id_in

        # Assemble update expression
        update_expression = "SET " + ", ".join(set_clauses)
        if remove_clauses:
            update_expression += " REMOVE " + ", ".join(remove_clauses)

        # ---- Build transactional batch ----
        transact_items = [{
            "Update": {
                "TableName": SONG_TABLE,
                "Key": {"musicId": {"S": music_id}},
                "UpdateExpression": update_expression,
                "ExpressionAttributeNames": {k: v for k, v in expr_attr_names.items()},
                "ExpressionAttributeValues": _marshal_expr_attr_vals(expr_attr_vals),
                "ReturnValuesOnConditionCheckFailure": "NONE"
            }
        }]

        # Determine final set of genres we want in the index
        final_genres_for_index = (
            desired_genres if desired_genres is not None else current_genres
        )
        to_add = set()
        to_remove = set()
        if desired_genres is not None:
            to_add = final_genres_for_index - current_genres
            to_remove = current_genres - final_genres_for_index

        # Add new index rows (carry new/unchanged albumId)
        for g in to_add:
            item = {
                "genre": {"S": g},
                "musicId": {"S": music_id},
                "createdAt": {"S": now}
            }
            if new_album_id_effective is not None:
                item["albumId"] = {"S": new_album_id_effective}
            transact_items.append({
                "Put": {
                    "TableName": MUSIC_BY_GENRE_TABLE,
                    "Item": item,
                    "ReturnValuesOnConditionCheckFailure": "NONE"
                }
            })

        # Delete removed index rows
        for g in to_remove:
            transact_items.append({
                "Delete": {
                    "TableName": MUSIC_BY_GENRE_TABLE,
                    "Key": {"genre": {"S": g}, "musicId": {"S": music_id}},
                    "ReturnValuesOnConditionCheckFailure": "NONE"
                }
            })

        # If album changed (or explicitly cleared/set), update albumId on all remaining genre rows
        if album_change_requested:
            remaining = final_genres_for_index - to_remove
            for g in remaining:
                if new_album_id_effective is None:
                    # Remove albumId attribute
                    transact_items.append({
                        "Update": {
                            "TableName": MUSIC_BY_GENRE_TABLE,
                            "Key": {"genre": {"S": g}, "musicId": {"S": music_id}},
                            "UpdateExpression": "REMOVE albumId",
                            "ReturnValuesOnConditionCheckFailure": "NONE"
                        }
                    })
                else:
                    transact_items.append({
                        "Update": {
                            "TableName": MUSIC_BY_GENRE_TABLE,
                            "Key": {"genre": {"S": g}, "musicId": {"S": music_id}},
                            "UpdateExpression": "SET albumId = :a",
                            "ExpressionAttributeValues": {":a": {"S": new_album_id_effective}},
                            "ReturnValuesOnConditionCheckFailure": "NONE"
                        }
                    })

        # Execute the transaction (chunk if needed)
        if len(transact_items) <= 25:
            dynamo_client.transact_write_items(TransactItems=transact_items)
        else:
            # Always write the SONG_TABLE update first
            dynamo_client.transact_write_items(TransactItems=[transact_items[0]])
            for batch in _chunked(transact_items[1:], 25):
                dynamo_client.transact_write_items(TransactItems=batch)

        # Prepare delta info (only if genres sent)
        if desired_genres is not None:
            genres_delta = {
                "previous": sorted(list(current_genres)),
                "final": sorted(list(final_genres_for_index)),
                "added": sorted(list(to_add)),
                "removed": sorted(list(to_remove)),
            }

        # Return updated song
        updated = song_table.get_item(Key={"musicId": music_id}).get("Item", {})
        return response(200, {
            "message": "Song updated successfully (genres & album synchronized)",
            "musicId": music_id,
            "updatedItem": updated,
            "genresDelta": genres_delta,
            "albumId": new_album_id_effective
        })

    except ClientError as e:
        return response(500, {"error": str(e)})
    except Exception as e:
        return response(500, {"error": str(e)})

import json
import os
import uuid
import base64
import mimetypes
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from common.queue import enqueue_recompute
from boto3.dynamodb.conditions import Key

# --- AWS Clients ---
dynamodb = boto3.resource("dynamodb")
dynamo_client = boto3.client("dynamodb")
s3 = boto3.client("s3")
sns = boto3.client("sns")
cognito_client = boto3.client("cognito-idp")

SONG_TABLE = os.environ["SONG_TABLE"]
MUSIC_BY_GENRE_TABLE = os.environ["MUSIC_BY_GENRE_TABLE"]
S3_BUCKET = os.environ["S3_BUCKET"]
TOPIC_ARN = os.environ["NOTIFICATIONS_TOPIC_ARN"]
USER_POOL_ID = os.environ["USER_POOL_ID"]
MUSIC_FOLDER = os.environ.get("MUSIC_FOLDER", "music")
COVERS_FOLDER = os.environ.get("COVERS_FOLDER", "covers")
SUBS_TABLE = os.environ["SUBSCRIPTIONS_TABLE"]
subscriptions_table = dynamodb.Table(SUBSCRIPTIONS_TABLE)
song_table = dynamodb.Table(SONG_TABLE)
subs_table = dynamodb.Table(SUBS_TABLE)



def response(status_code, body):
    """Uniform CORS + JSON response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
        },
        "body": json.dumps(body),
    }


def _put_object_to_s3(bucket, key, data, content_type):
    """Upload a binary object to S3 and return public URL."""
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    return f"https://{bucket}.s3.amazonaws.com/{key}"


def _chunked(iterable, size):
    """Yield lists of length <= size from iterable."""
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

# --- Get user email from Cognito ---
def get_user_email(user_id):
    resp = cognito_client.list_users(
        UserPoolId=os.environ["USER_POOL_ID"],
        Filter=f'sub = "{user_id}"'
    )
    if resp['Users']:
        for attr in resp['Users'][0]['Attributes']:
            if attr['Name'] == 'email':
                return attr['Value']
    return None


def get_subscribed_user_ids(subscription_type, target_id):
    """
    Returns a list of userIds who are subscribed to a given artist or genre.
    """
    print(f"Fetching subscribers for {subscription_type} {target_id}")
    try:
        resp = subscriptions_table.query(
            IndexName="SubscriptionTypeTargetIdIndex",
            KeyConditionExpression=(
                Key("subscriptionType").eq(subscription_type) &
                Key("targetId").eq(target_id)
            )
        )
        return [item["userId"] for item in resp.get("Items", [])]
    except Exception as e:
        print(f"Error fetching subscribers for {subscription_type} {target_id}: {e}")
        return []

def send_notification(title, artist_ids, genres):
    message = (
        f"🎵 New song released!\n\n"
        f"Title: {title}\n"
        f"Genres: {', '.join(genres)}\n"
        f"Artists: {', '.join(artist_ids)}"
    )

    sns.publish(
        TopicArn=os.environ["NOTIFICATIONS_TOPIC_ARN"],
        Subject="New Song Released!",
        Message=message
    )

# --- Send notifications to subscribed users ---
def send_notifications(artist_ids, genres, title):
    notified_emails = set()
    print(f"Sending notifications for new song '{title}' to subscribers of artists {artist_ids} and genres {genres}")
    
    # Collect user IDs
    user_ids = set()
    for artist_id in artist_ids:
        user_ids.update(get_subscribed_user_ids("artist", artist_id))
    for genre in genres:
        user_ids.update(get_subscribed_user_ids("genre", genre))
    print(f"Total unique subscribers to notify: {len(user_ids)}", user_ids)

    # Send SNS message to each email
    for user_id in user_ids:
        email = get_user_email(user_id)
        if email and email not in notified_emails:
            # Subscribe user to the topic (if not already subscribed)
            try:
                sns.subscribe(TopicArn=TOPIC_ARN, Protocol='email', Endpoint=email)
            except Exception as e:
                print(f"Could not subscribe {email}: {e}")

            # Publish the message
            sns.publish(
                TopicArn=TOPIC_ARN,
                Subject="New Song Released!",
                Message=f"🎵 New song released!\n\nTitle: {title}\nGenres: {', '.join(genres)}\nArtists: {', '.join(artist_ids)}"
            )
            notified_emails.add(email)

def lambda_handler(event, context):
    # --- Handle CORS preflight ---
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        # --- Parse body ---
        body_raw = event.get("body", "{}")
        body = json.loads(body_raw) if isinstance(body_raw, str) else (body_raw or {})

        title = body.get("title")
        file_name = body.get("fileName")
        file_content_base64 = body.get("fileContent")
        genres = body.get("genres", [])
        artist_ids = body.get("artistIds", [])

        album_id = body.get("albumId")
        cover_image_base64 = body.get("coverImage")

        # --- Validation ---
        if not title or not file_name or not file_content_base64 or not artist_ids or not genres:
            return response(400, {
                "error": "title, fileName, fileContent, artistIds, and genres are required"
            })
        if not isinstance(genres, list) or not all(isinstance(g, str) and g.strip() for g in genres):
            return response(400, {"error": "genres must be a non-empty list of strings"})
        if not isinstance(artist_ids, list) or not all(isinstance(a, str) and a.strip() for a in artist_ids):
            return response(400, {"error": "artistIds must be a non-empty list of strings"})

        # --- Upload audio to S3 ---
        content_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
        file_bytes = base64.b64decode(file_content_base64)
        music_key = f"{MUSIC_FOLDER}/{uuid.uuid4()}-{file_name}"
        music_url = _put_object_to_s3(S3_BUCKET, music_key, file_bytes, content_type)

        # --- Optional cover upload ---
        cover_url = None
        if cover_image_base64:
            cover_key = f"{COVERS_FOLDER}/{uuid.uuid4()}-cover.jpg"
            cover_bytes = base64.b64decode(cover_image_base64)
            cover_url = _put_object_to_s3(S3_BUCKET, cover_key, cover_bytes, "image/jpeg")

        # --- Canonical song record ---
        music_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        file_ext = (file_name.rsplit(".", 1)[-1] if "." in file_name else "").lower()

        actions = []

        # 1) SONG_TABLE
        actions.append({
            "Put": {
                "TableName": SONG_TABLE,
                "Item": {
                    "musicId": {"S": music_id},
                    "title": {"S": title},
                    "fileName": {"S": file_name},
                    "fileType": {"S": file_ext or "unknown"},
                    "fileSize": {"N": str(len(file_bytes))},
                    "createdAt": {"S": now},
                    "updatedAt": {"S": now},
                    "artistIds": {"L": [{"S": a} for a in artist_ids]},
                    "albumId": {"S": album_id} if album_id else {"NULL": True},
                    "fileUrl": {"S": music_url},
                    "coverUrl": {"S": cover_url} if cover_url else {"NULL": True},
                    "genres": {"L": [{"S": g} for g in genres]},
                },
                "ConditionExpression": "attribute_not_exists(musicId)",
            }
        })

        # 2) MUSIC_BY_GENRE_TABLE
        for genre in genres:
            item = {
                "genre": {"S": genre},
                "musicId": {"S": music_id},
                "createdAt": {"S": now},
            }
            if album_id:
                item["albumId"] = {"S": album_id}
            actions.append({"Put": {"TableName": MUSIC_BY_GENRE_TABLE, "Item": item}})

        ARTIST_INFO_TABLE = os.environ['ARTIST_INFO_TABLE']

        # 3) For each artist, append musicId to their songs list
        for artist_id in artist_ids:
            actions.append({
                "Update": {
                    "TableName": ARTIST_INFO_TABLE,
                    "Key": {
                        "artistId": {"S": artist_id}
                    },
                    "UpdateExpression": "SET #songs = list_append(if_not_exists(#songs, :empty_list), :new_song)",
                    "ExpressionAttributeNames": {
                        "#songs": "songs"
                    },
                    "ExpressionAttributeValues": {
                        ":new_song": {"L": [{"S": music_id}]},
                        ":empty_list": {"L": []}
                    }
                }
            })

        # DynamoDB TransactWriteItems has a limit of 25 actions per request.
        # If we exceed it (rare—only with many genres+artists), we split into chunks.
        # We always write the SONG_TABLE put first to ensure ID existence.
        # --- Write to DynamoDB (chunk if >25) ---
        if len(actions) <= 25:
            dynamo_client.transact_write_items(TransactItems=actions)
        else:
            dynamo_client.transact_write_items(TransactItems=[actions[0]])
            for batch in _chunked(actions[1:], 25):
                dynamo_client.transact_write_items(TransactItems=batch)

        # --- Recompute feed for subscribed users ---
        try:
            # notify users subscribed by genre
            for g in genres:
                resp = subs_table.query(
                    IndexName="SubscriptionTypeTargetIdIndex",
                    KeyConditionExpression=Key("subscriptionType").eq("genre") & Key("targetId").eq(g)
                )
                for item in resp.get("Items", []):
                    user_id = item["userId"]
                    enqueue_recompute(user_id, "new_song_genre", music_id)

            # notify users subscribed by artist
            for artist_id in artist_ids:
                resp = subs_table.query(
                    IndexName="SubscriptionTypeTargetIdIndex",
                    KeyConditionExpression=Key("subscriptionType").eq("artist") & Key("targetId").eq(artist_id)
                )
                for item in resp.get("Items", []):
                    user_id = item["userId"]
                    print("U music upload reloaduje feed za korisnika ",user_id," jer je subscribed na artista nove pesme ",artist_id)
                    enqueue_recompute(user_id, "new_song_artist", music_id)

        except Exception as e:
            pass

        # Success
        # --- Publish notification ---
        send_notifications(artist_ids, genres, title)

        return response(201, {
            "message": "Music content uploaded successfully (normalized)",
            "musicId": music_id,
            "title": title,
            "genres": genres,
            "albumId": album_id,
            "fileUrl": music_url,
            "coverUrl": cover_url,
        })

    except ClientError as e:
        return response(500, {"error": str(e)})
    except Exception as e:
        return response(500, {"error": str(e)})

from urllib.parse import quote
import boto3
import os
import uuid
import re

# --- AWS clients ---
ddb = boto3.client("dynamodb")
transcribe = boto3.client("transcribe")
s3 = boto3.client("s3")

# --- Environment variables ---
SONG_TABLE = os.environ["SONG_TABLE"]
SONG_BUCKET = os.environ["SONG_BUCKET"]

# --- Helpers ---
def sanitize_transcribe_key(name: str) -> str:
    """
    AWS Transcribe only allows letters, numbers, '.', '_', and '-'
    """
    return re.sub(r"[^0-9a-zA-Z._-]", "_", name)

# --- Lambda handler ---
def handler(event, context):
    if "Records" not in event:
        print("No Records in event")
        return {"ok": False, "reason": "No S3 records"}

    for rec in event["Records"]:
        # Extract S3 key
        key = rec.get("s3", {}).get("object", {}).get("key")
        if not key:
            print("Skipping record with no key:", rec)
            continue

        # Only process MP3 files in "music/" folder
        if not key.startswith("music/") or not key.lower().endswith(".mp3"):
            print("Skipping non-music file:", key)
            continue

        # Verify object exists
        try:
            s3.head_object(Bucket=SONG_BUCKET, Key=key)
        except s3.exceptions.ClientError as e:
            print("S3 object not found:", key, e)
            continue

        # Derive music ID from filename
        music_id = key.split("/")[-1].split(".")[0]
        safe_music_id = sanitize_transcribe_key(music_id)
        output_key = f"transcriptions/{safe_music_id}.json"

        # File URI for Transcribe
        file_uri = f"s3://{SONG_BUCKET}/{quote(key)}"
        job_name = f"transcribe-{safe_music_id}-{uuid.uuid4()}"

        print("Starting transcription for:", file_uri)
        try:
            transcribe.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={"MediaFileUri": file_uri},
                MediaFormat="mp3",
                LanguageCode="en-US",
                OutputBucketName=SONG_BUCKET,
                OutputKey=output_key
            )
        except Exception as e:
            print(f"Failed to start transcription for {file_uri}: {e}")
            continue

        # Mark transcript as pending in DynamoDB
        try:
            ddb.update_item(
                TableName=SONG_TABLE,
                Key={"musicId": {"S": music_id}},
                UpdateExpression="SET hasTranscript = :p",
                ExpressionAttributeValues={":p": {"BOOL": False}}
            )
        except Exception as e:
            print(f"Failed to mark transcript as pending for {music_id}: {e}")

    return {"ok": True}

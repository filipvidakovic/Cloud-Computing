import boto3
import os
import uuid
import re
import json
from urllib.parse import quote, unquote_plus

# --- AWS clients ---
ddb = boto3.client("dynamodb")
transcribe = boto3.client("transcribe")
s3 = boto3.client("s3")

# --- Environment variables ---
SONG_TABLE = os.environ["SONG_TABLE"]
SONG_BUCKET = os.environ["SONG_BUCKET"]

# --- Helpers ---
def sanitize_transcribe_key(name: str) -> str:
    return re.sub(r"[^0-9a-zA-Z._-]", "_", name)

def find_original_music_id(s3_key):
    """
    Find the original musicId in DynamoDB that matches this S3 key
    """
    print(f"🔍 Looking for original music ID for S3 key: {s3_key}")
    
    try:
        scan_response = ddb.scan(TableName=SONG_TABLE)
        all_songs = scan_response.get('Items', [])
        print(f"📊 Found {len(all_songs)} songs in DynamoDB")
        
        for song in all_songs:
            file_url = song.get('fileUrl', {}).get('S', '')
            music_id = song.get('musicId', {}).get('S', '')
            file_name = song.get('fileName', {}).get('S', '')
            
            print(f"🎵 Checking song: {music_id}")
            print(f"   File URL: {file_url}")
            print(f"   File Name: {file_name}")
            
            # Check if this S3 key is part of the fileUrl OR matches the filename
            if s3_key in file_url or os.path.basename(s3_key) in file_url:
                print(f"✅ FOUND MATCH! Original music_id: {music_id}")
                return music_id
        
        print(f"❌ No DynamoDB item found for S3 key: {s3_key}")
        return None
        
    except Exception as e:
        print(f"❌ Error finding original music_id: {e}")
        return None

# --- Lambda handler ---
def handler(event, context):
    print("=== START TRANSCRIPTION LAMBDA ===")
    print("Event:", json.dumps(event))

    if "Records" not in event:
        return {"ok": False, "reason": "No S3 records"}

    for rec in event["Records"]:
        raw_key = rec.get("s3", {}).get("object", {}).get("key")
        if not raw_key:
            continue

        key = unquote_plus(raw_key)
        print(f"🎵 Processing S3 key: {key}")

        # Only process MP3 files in the "music/" folder
        if not key.startswith("music/") or not key.lower().endswith(".mp3"):
            print(f"⏭️ Skipping non-music file: {key}")
            continue

        # FIX: Find the ORIGINAL music ID from DynamoDB
        original_music_id = find_original_music_id(key)
        if not original_music_id:
            print(f"❌ Could not find original music ID for: {key}")
            continue

        # Use the original music ID for transcription
        safe_music_id = sanitize_transcribe_key(original_music_id)
        output_key = f"transcriptions/{safe_music_id}.json"
        
        print(f"🎯 Using original music_id: {original_music_id}")
        print(f"🔒 Safe music_id for Transcribe: {safe_music_id}")

        # File URI for Transcribe
        file_uri = f"s3://{SONG_BUCKET}/{key}"
        job_name = f"transcribe-{safe_music_id}-{uuid.uuid4()}"

        try:
            transcribe.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={"MediaFileUri": file_uri},
                MediaFormat="mp3",
                LanguageCode="en-US",
                OutputBucketName=SONG_BUCKET,
                OutputKey=output_key,
            )
            print(f"✅ Transcription job started: {job_name}")
            
            # Mark transcript as pending in DynamoDB with ORIGINAL ID
            ddb.update_item(
                TableName=SONG_TABLE,
                Key={"musicId": {"S": original_music_id}},
                UpdateExpression="SET hasTranscript = :p",
                ExpressionAttributeValues={":p": {"BOOL": False}},
            )
            print(f"🟡 Marked transcript as pending for {original_music_id}")
            
        except Exception as e:
            print(f"❌ Failed to start transcription: {e}")
            continue

    print("=== END START TRANSCRIPTION LAMBDA ===")
    return {"ok": True}
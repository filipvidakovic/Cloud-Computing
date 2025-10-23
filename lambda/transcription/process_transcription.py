import boto3
import os
import json

ddb = boto3.client("dynamodb")
s3 = boto3.client("s3")

SONG_TABLE = os.environ["SONG_TABLE"]
SONG_BUCKET = os.environ["SONG_BUCKET"]

def handler(event, context):
    print("=== PROCESS TRANSCRIPTION LAMBDA ===")
    try: 
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            print(f"📄 Processing transcription file: {key}")

            # Get the transcription JSON from S3
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                data = json.loads(obj["Body"].read())
                transcript_text = data["results"]["transcripts"][0]["transcript"]
                print(f"📝 Extracted transcript: {transcript_text}")
            except Exception as e:
                print(f"❌ Failed to read transcription: {e}")
                continue

            original_music_id = os.path.basename(key).replace(".json", "")
            print(f"🆔 Updating music_id: {original_music_id}")

            # Update DynamoDB with ORIGINAL music ID
            try:
                response = ddb.update_item(
                    TableName=SONG_TABLE,
                    Key={"musicId": {"S": original_music_id}},
                    UpdateExpression="SET hasTranscript = :t, transcriptText = :txt",
                    ExpressionAttributeValues={
                        ":t": {"BOOL": True},
                        ":txt": {"S": transcript_text}
                    }
                )
                print(f"✅ Successfully updated transcription for: {original_music_id}")
                
            except Exception as db_error:
                print(f"❌ Failed to update DynamoDB: {db_error}")

        return {"ok": True}
        
    except Exception as e:
        print(f"💥 PROCESS TRANSCRIPTION ERROR: {e}")
        return {"error": str(e)}
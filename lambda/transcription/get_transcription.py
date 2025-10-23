import boto3
import os
import json

# --- AWS clients ---
ddb = boto3.client("dynamodb")
s3 = boto3.client("s3")

# --- Environment variables ---
SONG_TABLE = os.environ["SONG_TABLE"]
SONG_BUCKET = os.environ["SONG_BUCKET"]

def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Access-Control-Allow-Credentials": "true"
    }

def handler(event, context):
    print("=== GET TRANSCRIPTION LAMBDA ===")
    
    # Handle OPTIONS request for CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": cors_headers(),
            "body": json.dumps({"message": "CORS preflight"})
        }
    
    try:
        # Extract songId from path parameters
        path_params = event.get("pathParameters", {}) or {}
        song_id = path_params.get("songId")
        
        print(f"Looking for transcription for song: {song_id}")

        if not song_id:
            return {
                "statusCode": 400,
                "headers": cors_headers(),
                "body": json.dumps({"error": "Missing songId parameter"})
            }

        # Get item from DynamoDB
        item = ddb.get_item(
            TableName=SONG_TABLE,
            Key={"musicId": {"S": song_id}}
        ).get("Item")

        if not item:
            return {
                "statusCode": 404,
                "headers": cors_headers(),
                "body": json.dumps({"error": "Song not found"})
            }

        print("DynamoDB Item found")
        
        # Check if transcriptText exists
        transcription = item.get("transcriptText", {}).get("S", "")
        has_transcript = item.get("hasTranscript", {}).get("BOOL", False)
        
        print(f"Transcription present: {bool(transcription)}")
        print(f"Has transcript flag: {has_transcript}")

        if transcription:
            print(f"Returning transcription for: {song_id}")
            return {
                "statusCode": 200,
                "headers": cors_headers(),
                "body": json.dumps({"transcription": transcription})
            }
        else:
            print(f"❌ No transcription found for: {song_id}")
            return {
                "statusCode": 404,
                "headers": cors_headers(),
                "body": json.dumps({"error": "Transcription not available yet"})
            }

    except Exception as e:
        print(f"GET TRANSCRIPTION ERROR: {e}")
        return {
            "statusCode": 500,
            "headers": cors_headers(),
            "body": json.dumps({"error": str(e)})
        }
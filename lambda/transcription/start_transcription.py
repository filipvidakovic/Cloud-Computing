import boto3, os, uuid, re

ddb = boto3.client("dynamodb")
transcribe = boto3.client("transcribe")

SONG_TABLE = os.environ["SONG_TABLE"]
SONG_BUCKET = os.environ["SONG_BUCKET"]
TRANSCRIPTIONS_BUCKET = os.environ["TRANSCRIPTIONS_BUCKET"]

def sanitize_name(name: str) -> str:
    # Allowed for TranscribeJobName: letters, numbers, ._- 
    return re.sub(r"[^0-9a-zA-Z._-]", "_", name)

def handler(event, context):
    for rec in event["Records"]:
        key = rec["s3"]["object"]["key"]
        music_id = key.split("/")[-1].split(".")[0]

        # Sanitize job name and output key
        safe_music_id = sanitize_name(music_id)
        job_name = f"transcribe-{safe_music_id}-{uuid.uuid4()}"
        file_uri = f"s3://{SONG_BUCKET}/{key}"
        output_key = f"{safe_music_id}.json"

        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={"MediaFileUri": file_uri},
            MediaFormat="mp3",
            LanguageCode="en-US",
            OutputBucketName=TRANSCRIPTIONS_BUCKET,
            OutputKey=output_key
        )

        # Mark transcript as pending
        ddb.update_item(
            TableName=SONG_TABLE,
            Key={"musicId": {"S": music_id}},
            UpdateExpression="SET hasTranscript = :p",
            ExpressionAttributeValues={":p": {"BOOL": False}}
        )

    return {"ok": True}

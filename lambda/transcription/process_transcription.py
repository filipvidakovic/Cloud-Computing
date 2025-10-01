import boto3, os, json

ddb = boto3.client("dynamodb")
SONG_TABLE = os.environ["SONG_TABLE"]

def handler(event, context):
    s3 = boto3.client("s3")
    
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        obj = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read())

        transcript_text = data["results"]["transcripts"][0]["transcript"]

        # Extract music_id safely (remove any folder prefix and .json suffix)
        music_id = key.split("/")[-1].replace(".json", "")

        ddb.update_item(
            TableName=SONG_TABLE,
            Key={"musicId": {"S": music_id}},
            UpdateExpression="SET hasTranscript = :t, transcriptText = :txt",
            ExpressionAttributeValues={
                ":t": {"BOOL": True},
                ":txt": {"S": transcript_text}
            }
        )

    return {"ok": True}

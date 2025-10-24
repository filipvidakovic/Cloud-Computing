# lambda/music/music_signed_get.py
import json, os, urllib.parse
import boto3
from botocore.exceptions import ClientError

SONG_TABLE = os.environ["SONG_TABLE"]
S3_BUCKET  = os.environ["S3_BUCKET"]  # your upload bucket (fallback)
SIGNED_URL_TTL_SECONDS = int(os.environ.get("SIGNED_URL_TTL_SECONDS", "900"))

dynamodb = boto3.resource("dynamodb")
song_table = dynamodb.Table(SONG_TABLE)
s3 = boto3.client("s3")

def _cors(body, status=200):
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET",
        },
        "body": json.dumps(body),
    }

def _extract_bucket_and_key_from_url(url: str):
    """
    Supports:
      https://<bucket>.s3.amazonaws.com/<key>
      https://<bucket>.s3.<region>.amazonaws.com/<key>
      https://<bucket>.s3-accelerate.amazonaws.com/<key>
      https://<bucket>.s3.dualstack.<region>.amazonaws.com/<key>
      https://s3.amazonaws.com/<bucket>/<key>
      https://s3.<region>.amazonaws.com/<bucket>/<key>
    Returns (bucket, key) or (None, None) if unsupported.
    """
    if not url:
        return (None, None)

    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc or ""
    path = (parsed.path or "").lstrip("/")  # may be "<key>" or "<bucket>/<key>" in path-style
    host_parts = host.split(".")

    # Path-style: host starts with s3 or s3-<region>
    if host_parts and host_parts[0].startswith("s3"):
        # Expect path = "<bucket>/<key>"
        if "/" not in path:
            return (None, None)
        bucket, key = path.split("/", 1)
        return (bucket, urllib.parse.unquote(key))

    # Virtual-hosted: "<bucket>.s3[.*].amazonaws.com"
    # e.g., my-bucket.s3.amazonaws.com, my-bucket.s3.eu-central-1.amazonaws.com
    if len(host_parts) >= 3 and host_parts[1].startswith("s3"):
        bucket = host_parts[0]
        key = urllib.parse.unquote(path)
        return (bucket, key)

    # Accelerate: "<bucket>.s3-accelerate.amazonaws.com"
    if len(host_parts) >= 3 and host_parts[1].startswith("s3-accelerate"):
        bucket = host_parts[0]
        key = urllib.parse.unquote(path)
        return (bucket, key)

    # Dualstack virtual-hosted: "<bucket>.s3.dualstack.<region>.amazonaws.com"
    # covered by host_parts[1] == "s3" and host_parts[2] == "dualstack" path as above
    # (already matched by the len>=3 & host_parts[1].startswith("s3") condition)

    return (None, None)

def lambda_handler(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return _cors({})

    qs = event.get("queryStringParameters") or {}
    music_id = qs.get("musicId")
    if not music_id:
        return _cors({"error": "musicId is required"}, 400)

    try:
        # 1) Load record
        resp = song_table.get_item(Key={"musicId": music_id})
        item = resp.get("Item")
        if not item:
            return _cors({"error": "Not found"}, 404)

        file_url = item.get("fileUrl")
        if not file_url:
            return _cors({"error": "No fileUrl on item"}, 500)

        # 2) Try to parse bucket+key from fileUrl
        bucket, key = _extract_bucket_and_key_from_url(file_url)
        if not key:
            # fallback assumption: your objects are in S3_BUCKET and fileUrl path is "/<key>"
            # This mirrors your original _put_object_to_s3 return format
            # e.g. https://<bucket>.s3.amazonaws.com/music/uuid-filename.mp3
            # If parse failed but you know it's the same bucket, try path as key:
            key = urllib.parse.urlparse(file_url).path.lstrip("/")
            bucket = bucket or S3_BUCKET

        if not bucket or not key:
            return _cors({"error": "Could not extract S3 object key from fileUrl"}, 500)

        # 3) Optional nicer content-type
        ext = (item.get("fileType") or "").lower()
        content_type = {
            "mp3":"audio/mpeg","m4a":"audio/mp4","aac":"audio/aac",
            "wav":"audio/wav","ogg":"audio/ogg","flac":"audio/flac",
        }.get(ext)

        params = {"Bucket": bucket, "Key": key}
        if content_type:
            params["ResponseContentType"] = content_type

        signed = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params=params,
            ExpiresIn=SIGNED_URL_TTL_SECONDS,
        )
        return _cors({"fileUrlSigned": signed})

    except ClientError as e:
        return _cors({"error": str(e)}, 500)
    except Exception as e:
        return _cors({"error": str(e)}, 500)

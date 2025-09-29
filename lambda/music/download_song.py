# download_song.py
import os, json
import boto3
from urllib.parse import urlparse, quote

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

SONG_TABLE = os.environ["SONG_TABLE"]
S3_BUCKET  = os.environ["S3_BUCKET"]
FRONTEND_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "*")
PRESIGN_EXPIRES = int(os.environ.get("PRESIGN_EXPIRES", "86400"))  # 24h

song_table = dynamodb.Table(SONG_TABLE)

def _cors_headers():
  return {
    "Access-Control-Allow-Origin": FRONTEND_ORIGIN,
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Vary": "Origin",
  }

def _extract_key_from_url(u: str | None) -> str | None:
  if not u:
    return None
  p = urlparse(u)
  path = (p.path or "").lstrip("/")
  if p.netloc.startswith(f"{S3_BUCKET}.") or p.netloc == S3_BUCKET:
    return path or None
  if path.startswith(f"{S3_BUCKET}/"):
    return path.split("/", 1)[1] or None
  return path or None

def lambda_handler(event, context):
  method = event.get("httpMethod")
  if method == "OPTIONS":
    return {"statusCode": 200, "headers": _cors_headers(), "body": ""}

  if method != "GET":
    return {"statusCode": 405, "headers": _cors_headers(), "body": json.dumps({"error":"Method not allowed"})}

  qs = event.get("queryStringParameters") or {}
  music_id = qs.get("musicId")
  if not music_id:
    return {"statusCode": 400, "headers": _cors_headers(), "body": json.dumps({"error":"musicId is required"})}

  # Lookup
  it = song_table.get_item(Key={"musicId": music_id}).get("Item")
  if not it:
    return {"statusCode": 404, "headers": _cors_headers(), "body": json.dumps({"error":"Not found"})}

  key = _extract_key_from_url(it.get("fileUrl"))
  if not key:
    return {"statusCode": 500, "headers": _cors_headers(), "body": json.dumps({"error":"Missing file key"})}

  fname = it.get("fileName") or os.path.basename(key)
  # Response Content-Disposition to force download:
  presigned = s3.generate_presigned_url(
    "get_object",
    Params={
      "Bucket": S3_BUCKET,
      "Key": key,
      "ResponseContentDisposition": f'attachment; filename="{quote(fname)}"',
    },
    ExpiresIn=PRESIGN_EXPIRES,
  )

  # 302 redirect to S3 — avoids binary streaming through API GW
  return {
    "statusCode": 302,
    "headers": {
      **_cors_headers(),
      "Location": presigned,
      "Cache-Control": "no-store",
    },
    "body": "",
  }

from collections import Counter, defaultdict
from decimal import Decimal
from itertools import islice
import boto3, json, os, time
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
ddbc = dynamodb.meta.client

FEED_TABLE_NAME        = os.environ.get("USER_FEED_TABLE",        "UserFeedTable")
HISTORY_TABLE_NAME     = os.environ.get("USER_HISTORY_TABLE",     "UserHistoryTable")
REACTIONS_TABLE_NAME   = os.environ.get("USER_REACTIONS_TABLE",   "UserReactionsTable")
SUBS_TABLE_NAME        = os.environ.get("USER_SUBSCRIPTIONS_TABLE","UserSubscriptionsTable")
GENRE_INDEX_TABLE_NAME = os.environ.get("MUSIC_TABLE",            "MusicTable")   # PK=genre, SK=musicId
SONG_TABLE_NAME        = os.environ.get("SONG_TABLE",             "SongTable")    # PK=musicId
ARTIST_INFO_TABLE_NAME = os.environ.get("ARTIST_INFO_TABLE",      "ArtistInfoTable")

feed_table        = dynamodb.Table(FEED_TABLE_NAME)
history_table     = dynamodb.Table(HISTORY_TABLE_NAME)
reactions_table   = dynamodb.Table(REACTIONS_TABLE_NAME)
subs_table        = dynamodb.Table(SUBS_TABLE_NAME)
genre_index_table = dynamodb.Table(GENRE_INDEX_TABLE_NAME)
song_table        = dynamodb.Table(SONG_TABLE_NAME)
artist_info_table = dynamodb.Table(ARTIST_INFO_TABLE_NAME)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)

def load_subscriptions(user_id: str):
    # Your SubscriptionsTable stores one row per subscription (query, not get_item)
    items = subs_table.query(
        KeyConditionExpression=Key("userId").eq(user_id)
    ).get("Items", [])
    artists = {it["targetId"] for it in items if it.get("subscriptionType") == "artist"}
    genres  = {it["targetId"] for it in items if it.get("subscriptionType") == "genre"}
    return artists, genres

def load_reactions(user_id: str):
    items = reactions_table.query(
        KeyConditionExpression=Key("userId").eq(user_id)
    ).get("Items", [])
    # map[musicId] = "love" | "like" | "dislike"
    return {it["musicId"]: it.get("rate") for it in items}

def load_history(user_id: str):
    item = history_table.get_item(Key={"userId": user_id}).get("Item") or {}
    recent = item.get("recentPlays", []) or []
    genre_counts = Counter([p.get("genre") for p in recent if p.get("genre")])
    return recent, genre_counts

def paginate_genre(genre: str, per_page=200, max_items=1000):
    """Yield musicIds from the genre index table (PK=genre, SK=musicId)."""
    last = None
    count = 0
    while True:
        kwargs = {
            "KeyConditionExpression": Key("genre").eq(genre),
            "Limit": per_page
        }
        if last:
            kwargs["ExclusiveStartKey"] = last
        resp = genre_index_table.query(**kwargs)
        for it in resp.get("Items", []):
            mid = it.get("musicId")
            if mid:
                yield mid
                count += 1
                if count >= max_items:
                    return
        last = resp.get("LastEvaluatedKey")
        if not last:
            break

def batch_get_songs(music_ids, chunk=100):
    """BatchGetItem from SONG_TABLE (PK=musicId). Returns dict[musicId] = song_item."""
    out = {}
    it = iter(set(music_ids))
    while True:
        batch = list(islice(it, chunk))
        if not batch:
            break

        req = {
            SONG_TABLE_NAME: {
                "Keys": [{"musicId": mid} for mid in batch]
            }
        }

        resp = ddbc.batch_get_item(RequestItems=req)

        for item in resp.get("Responses", {}).get(SONG_TABLE_NAME, []):
            mid = item["musicId"]
            out[mid] = {
                "musicId": mid,
                "artistIds": item.get("artistIds", []),
                "genres": item.get("genres", []),
            }

        retries = 0
        while resp.get("UnprocessedKeys") and retries < 5:
            retries += 1
            resp = ddbc.batch_get_item(RequestItems=resp["UnprocessedKeys"])
            for item in resp.get("Responses", {}).get(SONG_TABLE_NAME, []):
                mid = item["musicId"]
                out[mid] = {
                    "musicId": mid,
                    "artistIds": item.get("artistIds", []),
                    "genres": item.get("genres", []),
                }

    return out


def get_artist_song_ids(artist_id: str):
    artist = artist_info_table.get_item(Key={"artistId": artist_id}).get("Item")
    if not artist:
        return []
    songs = artist.get("songs", [])
    # allow both ["id1","id2"] or [{"musicId":"id1"}, ...]
    ids = []
    for s in songs:
        if isinstance(s, str):
            ids.append(s)
        elif isinstance(s, dict) and "musicId" in s:
            ids.append(s["musicId"])
    return ids

def calculate_score(song, sub_artists, sub_genres, reactions_map, genre_counts):
    score = 0.0

    song_genres = set(song.get("genres") or [])
    song_artists = set(song.get("artistIds", []))

    # +7 for each genre of the song user is subscribed to
    genre_matches = sub_genres.intersection(song_genres)
    score += 7 * len(genre_matches)

    # +15 for each artist of the song user is subscribed to
    artist_matches = song_artists.intersection(sub_artists)
    score += 15 * len(artist_matches)

    # listening history frequency (for genre)
    history_boost_applied = sum(genre_counts.get(g, 0) * 0.7 for g in song_genres)
    score += history_boost_applied
    history_boost_applied = Decimal(str(round(history_boost_applied, 4)))

    # reactions
    rxn = reactions_map.get(song["musicId"])
    if rxn == "love":
        score += 20
    elif rxn == "like":
        score += 10
    elif rxn == "dislike":
        score -= 200

    return score, {
        "genreMatches": list(genre_matches),
        "artistMatches": list(artist_matches),
        "historyBoostApplied": history_boost_applied,
        "reaction": rxn,
        "genres": list(song_genres),
    }

def lambda_handler(event, context):
    try:
        user_id = event["userId"]
        now = int(time.time())

        sub_artists, sub_genres = load_subscriptions(user_id)
        reactions_map = load_reactions(user_id)
        recent_plays, genre_counts = load_history(user_id)

        sub_genres = set(sub_genres or [])
        sub_artists = set(sub_artists or [])

        # Candidate musicIds
        candidate_ids = set()

        #genres user is interested in (subscribed to or listened to recently)
        merged_genres = sub_genres | {g for g in genre_counts.keys() if g}

        # 1) songs from genres user is interested in
        for g in merged_genres:
            for mid in paginate_genre(g, per_page=200, max_items=200):
                candidate_ids.add(mid)

        # 2) songs from artists user subscribed to
        for aid in sub_artists:
            candidate_ids.update(get_artist_song_ids(aid))

        # 3) songs with reactions from user
        candidate_ids.update(reactions_map.keys())

        if not candidate_ids:
            return {"statusCode": 200, "body": json.dumps({"feedCount": 0})}

        # fetch full song info for candidate songs
        songs = batch_get_songs(candidate_ids)

        # score
        # scoring loop (use the tuple returned by calculate_score)
        scored = []
        for song in songs.values():
            s, details = calculate_score(song, sub_artists, sub_genres, reactions_map, genre_counts)
            scored.append({
                "userId": user_id,
                "musicId": song["musicId"],
                "score": Decimal(str(round(s, 4))),
                "reason": details,
                "createdAt": now
            })

        scored.sort(key=lambda x: float(x["score"]), reverse=True)
        top50 = scored[:50]

        # delete old feed before writing new
        clear_old_feed(user_id)
        print("Found ", len(top50), " top 50 songs")
        # save top 50 songs to users feed
        with feed_table.batch_writer() as batch:
            for item in top50:
                batch.put_item(Item=item)

        return {"statusCode": 200, "body": json.dumps({"feedCount": len(top50)}, cls=DecimalEncoder)}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

def lambda_sqs_handler(event, context):
    print("📥 SQS Event:", json.dumps(event))  # log entire payload

    # groups messages by user
    by_user = defaultdict(list)
    for rec in event.get("Records", []):
        print("➡️ Raw record:", rec)  # log each record

        try:
            msg = json.loads(rec["body"])
            print("✅ Parsed message:", msg)

            uid = msg.get("userId")
            if uid:
                by_user[uid].append(msg)
        except Exception as e:
            print("❌ Failed to parse record body:", rec.get("body"), e)
            raise

    # recompute for each user once
    for user_id in by_user.keys():
        print(f"Call recompute for {user_id}")
        result = lambda_handler({"userId": user_id}, context)



def clear_old_feed(user_id: str):
    """
    Delete all existing feed items for a user before recomputing.
    """
    resp = feed_table.query(
        KeyConditionExpression=Key("userId").eq(user_id),
        ProjectionExpression="userId, musicId"
    )
    items = resp.get("Items", [])

    with feed_table.batch_writer() as batch:
        for it in items:
            batch.delete_item(Key={"userId": it["userId"], "musicId": it["musicId"]})

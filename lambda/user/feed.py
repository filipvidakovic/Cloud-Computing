from collections import Counter

import boto3
import json
import time
from boto3.dynamodb.conditions import Key
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")

feed_table = dynamodb.Table("UserFeedTable")
history_table = dynamodb.Table("UserHistoryTable")
reactions_table = dynamodb.Table("UserReactionsTable")
subs_table = dynamodb.Table("UserSubscriptionsTable")
music_table = dynamodb.Table("MusicTable")
artist_info_table = dynamodb.Table("ArtistInfoTable")


# Decimal encoder for safe JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)


def calculate_score(music, subscriptions, reactions, genre_counts):
    score = 0

    # Subscriptions
    if music.get("genre") in subscriptions.get("subscribedGenres", []):
        score += 10
    for artist_id in subscriptions.get("subscribedArtists", []):
        if artist_id in music.get("artistIds", []):
            score += 8

    # Listening history (weighted by frequency)
    g = music.get("genre")
    if g in genre_counts:
        score += genre_counts[g]*0.5

    # Reactions
    reaction = reactions.get(music["musicId"])
    if reaction == "love":
        score += 10
    elif reaction == "like":
        score += 5
    elif reaction == "dislike":
        score -= 10

    return score



def lambda_handler(event, context):
    try:
        user_id = event["userId"]

        # --- Subscriptions ---
        subs = subs_table.get_item(Key={"userId": user_id}).get("Item", {})
        if not subs:
            subs = {"subscribedGenres": [], "subscribedArtists": []}

        # reactions
        reaction_items = reactions_table.query(
            KeyConditionExpression=Key("userId").eq(user_id)
        ).get("Items", [])
        reactions = {r["musicId"]: r["reaction"] for r in reaction_items}

        # history
        history = history_table.get_item(Key={"userId": user_id}).get("Item", {})
        genre_counts = Counter([h["genre"] for h in history.get("recentPlays", [])])

        candidates = {}

        # 1. Songs from subscribed genres (query GSI)
        #should adjust to get music from music info table
        for g in subs.get("subscribedGenres", []):
            resp = music_table.query(
                IndexName="GenreIndex",
                KeyConditionExpression=Key("genre").eq(g),
                Limit=100
            )
            for m in resp.get("Items", []):
                candidates[m["musicId"]] = m

        # 2. Songs from subscribed artists (use songs field in ArtistInfoTable)
        #should adjust to get musci from music info table
        for artist_id in subs.get("subscribedArtists", []):
            artist = artist_info_table.get_item(Key={"artistId": artist_id}).get("Item")
            if artist:
                for mid in artist.get("songs", []):
                    # fetch each song to get metadata (genre, etc.)
                    song = music_table.get_item(Key={"musicId": mid}).get("Item")
                    if song:
                        candidates[mid] = song

        # 3. Songs the user reacted to (ensure included even if not in above)
        #should adjust to get music from music info table
        for mid in reactions.keys():
            song = music_table.get_item(Key={"musicId": mid}).get("Item")
            if song:
                candidates[mid] = song

        # --- Score all candidates ---
        scored = []
        for music in candidates.values():
            s = calculate_score(music, subs, reactions, recent_genres)
            scored.append({
                "userId": user_id,
                "musicId": music["musicId"],
                "score": s,
                "reason": "auto",
                "createdAt": int(time.time())
            })

        # --- Pick top 50 ---
        scored.sort(key=lambda x: x["score"], reverse=True)
        top50 = scored[:50]

        # --- Save to FeedTable ---
        with feed_table.batch_writer() as batch:
            for item in top50:
                batch.put_item(Item=item)

        return {
            "statusCode": 200,
            "body": json.dumps({"feedCount": len(top50)}, cls=DecimalEncoder)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

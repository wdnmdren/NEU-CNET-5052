# step2_crawl_batch.py
# Usage: python step2_crawl_batch.py 00

import os
import sys
import time
import random
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import requests
import pandas as pd
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError

from config import *

HEADERS = {"X-Riot-Token": API_KEY}

# -----------------------
# Utils
# -----------------------
def unix_days_ago(days: int) -> int:
    return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())

def sleep_jitter(mult=1.0):
    time.sleep(max(0.0, BASE_SLEEP * mult + random.random() * JITTER))

class RetryableHTTP(RuntimeError):
    pass

def robust_get_json(session: requests.Session, url: str, params=None, max_retries=10):
    backoff = 0.6
    for attempt in range(max_retries):
        try:
            r = session.get(url, headers=HEADERS, params=params, timeout=30)
            if r.status_code == 200:
                sleep_jitter()
                return r.json()
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                time.sleep(float(ra) if ra else 1.5)
                continue
            if 500 <= r.status_code < 600:
                time.sleep(backoff)
                backoff = min(12, backoff * 1.6)
                continue
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        except (ConnectionError, Timeout, ChunkedEncodingError) as e:
            time.sleep(backoff)
            backoff = min(15, backoff * 1.7)
            if attempt == max_retries - 1:
                raise RetryableHTTP(str(e))
            session = requests.Session()
    raise RetryableHTTP("max retries exceeded")

# -----------------------
# Riot API
# -----------------------
def recent_match_ids(session, puuid, start_time_unix, k):
    url = f"https://{REGION_ROUTING}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    params = {
        "queue": QUEUE_ID,
        "startTime": start_time_unix,
        "start": 0,
        "count": k
    }
    return robust_get_json(session, url, params=params) or []

def match_detail(session, match_id):
    url = f"https://{REGION_ROUTING}.api.riotgames.com/lol/match/v5/matches/{match_id}"
    return robust_get_json(session, url)

# -----------------------
# Seen matchId (global)
# -----------------------
_lock = threading.Lock()

def seen_path():
    return os.path.join(OUT_DIR, SEEN_MATCHES_FILE)

def load_seen_matches():
    if not os.path.exists(seen_path()):
        return set()
    with open(seen_path(), "r") as f:
        return set(x.strip() for x in f if x.strip())

def append_seen(match_id):
    with _lock:
        with open(seen_path(), "a") as f:
            f.write(match_id + "\n")

# -----------------------
# Parse match → undirected edges
# -----------------------
def parse_match_undirected(md):
    info = md["info"]
    meta = md["metadata"]
    mid = meta["matchId"]
    qid = info.get("queueId", -1)
    gst = info.get("gameStartTimestamp", 0)

    team_map = defaultdict(list)
    party_map = {}
    win_map = {}

    for p in info["participants"]:
        pu = p["puuid"]
        team = int(p["teamId"])
        team_map[team].append(pu)
        party_map[pu] = p.get("partyId")
        win_map[pu] = bool(p.get("win"))

    edges, players, mates = [], [], {}

    for team, puuids in team_map.items():
        puuids = sorted(puuids)

        for pu in puuids:
            players.append({
                "puuid": pu,
                "matchId": mid,
                "queueId": qid,
                "gameStartTimestamp": gst,
                "teamId": team,
                "win": win_map[pu],
            })
            mates[pu] = [x for x in puuids if x != pu]

        for i in range(len(puuids)):
            for j in range(i + 1, len(puuids)):
                u, v = puuids[i], puuids[j]
                if FILTER_SAME_PARTY_EDGES:
                    if party_map[u] is not None and party_map[u] == party_map[v]:
                        continue
                edges.append({
                    "u_puuid": u,
                    "v_puuid": v,
                    "matchId": mid,
                    "queueId": qid,
                    "gameStartTimestamp": gst,
                    "teamId": team,
                    "team_win": win_map[u],
                })

    return edges, players, mates

# -----------------------
# Main
# -----------------------
def main():
    batch = sys.argv[1]
    start_time = unix_days_ago(DAYS_BACK)

    with open(os.path.join(OUT_DIR, f"frontier_{batch}.txt")) as f:
        seeds = [x.strip() for x in f if x.strip()]

    seen = load_seen_matches()
    session = requests.Session()

    edges_all, players_all, next_frontier = [], [], []

    for puuid in seeds:
        try:
            mids = recent_match_ids(session, puuid, start_time, RECENT_MATCH_K)
            all_mates = set()   # ⭐ 收集 20 场的并集队友
            for mid in mids:
                if mid in seen:
                    continue
                seen.add(mid)
                append_seen(mid)
            
                md = match_detail(session, mid)
                edges, players, mates = parse_match_undirected(md)
            
                edges_all.extend(edges)
                players_all.extend(players)
            
                # 只收集队友，不立刻扩展
                all_mates.update(mates.get(puuid, []))
            
            # ⭐ 在 20 场的并集队友里，只扩展一次
            all_mates = list(all_mates)
            random.shuffle(all_mates)
            next_frontier.extend(all_mates[:MAX_NEW_MATES_PER_PLAYER])

        except Exception as e:
            print("[warn]", puuid, e)

    pd.DataFrame(edges_all).to_csv(os.path.join(OUT_DIR, f"edges_part_{batch}.csv"), index=False)
    pd.DataFrame(players_all).to_csv(os.path.join(OUT_DIR, f"players_part_{batch}.csv"), index=False)

    next_frontier = list(dict.fromkeys(next_frontier))
    with open(os.path.join(OUT_DIR, f"frontier_{int(batch)+1:02d}.txt"), "w") as f:
        for x in next_frontier:
            f.write(x + "\n")

    print(f"[done] batch={batch} edges={len(edges_all)} next={len(next_frontier)}")

if __name__ == "__main__":
    main()
